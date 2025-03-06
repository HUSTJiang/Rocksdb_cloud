#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CompletedPart.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <iostream>
#include <vector>
#include <thread>
#include <future>
#include <chrono>
#include <algorithm>
#include <mutex>

// 在内存中生成一个指定大小的数据（以 'A' 填充）
std::vector<char> GenerateMemoryFile(size_t size_in_mb) {
    size_t total_size = size_in_mb * 1024 * 1024;
    return std::vector<char>(total_size, 'A');
}

void UploadMemoryFileToS3(const std::string& bucket_name, const std::string& object_key,
                            const std::vector<char>& buffer, size_t part_size, size_t thread_count) {
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    {
        Aws::S3::S3Client s3_client;

        // Step 1: Initiate multipart upload
        Aws::S3::Model::CreateMultipartUploadRequest create_request;
        create_request.WithBucket(bucket_name).WithKey(object_key);

        auto create_response = s3_client.CreateMultipartUpload(create_request);
        if (!create_response.IsSuccess()) {
            std::cerr << "Failed to initiate multipart upload: " 
                      << create_response.GetError().GetMessage() << std::endl;
            Aws::ShutdownAPI(options);
            return;
        }
        std::string upload_id = create_response.GetResult().GetUploadId();

        // 记录上传开始时间
        auto start_time = std::chrono::high_resolution_clock::now();

        std::vector<Aws::S3::Model::CompletedPart> completed_parts;
        std::mutex completed_parts_mutex;
        size_t part_number = 1;
        size_t offset = 0;
        size_t total_size = buffer.size();

        // 定义上传每个分段的 lambda 函数
        auto upload_part = [&](size_t part_number, const std::vector<char>& part_buffer) {
            Aws::S3::Model::UploadPartRequest part_request;
            part_request.WithBucket(bucket_name)
                        .WithKey(object_key)
                        .WithUploadId(upload_id)
                        .WithPartNumber(part_number);

            auto stream = Aws::MakeShared<Aws::StringStream>("UploadPartStream");
            stream->write(part_buffer.data(), part_buffer.size());
            part_request.SetBody(stream);
            part_request.SetContentLength(part_buffer.size());

            auto part_response = s3_client.UploadPart(part_request);
            if (part_response.IsSuccess()) {
                Aws::S3::Model::CompletedPart completed_part;
                completed_part.WithPartNumber(part_number)
                              .WithETag(part_response.GetResult().GetETag());
                std::lock_guard<std::mutex> lock(completed_parts_mutex);
                completed_parts.push_back(completed_part);
                std::cout << "Uploaded part " << part_number << " successfully." << std::endl;
            } else {
                throw std::runtime_error("Failed to upload part " + std::to_string(part_number) +
                                           ": " + part_response.GetError().GetMessage());
            }
        };

        std::vector<std::future<void>> futures;
        // 分段上传：直接从内存缓冲区中按 part_size 分割数据
        while (offset < total_size) {
            size_t current_part_size = std::min(part_size, total_size - offset);
            std::vector<char> part_buffer(buffer.begin() + offset, buffer.begin() + offset + current_part_size);

            if (futures.size() >= thread_count) {
                futures.front().get();
                futures.erase(futures.begin());
            }
            futures.push_back(std::async(std::launch::async, upload_part, part_number, part_buffer));
            part_number++;
            offset += current_part_size;
        }

        // 等待所有任务完成
        for (auto& future : futures) {
            future.get();
        }

        // Step 3: Complete the multipart upload
        // 上传完成前，需要确保各分段按照 PartNumber 升序排列
        std::sort(completed_parts.begin(), completed_parts.end(), 
                [](const Aws::S3::Model::CompletedPart& a, const Aws::S3::Model::CompletedPart& b) {
                    return a.GetPartNumber() < b.GetPartNumber();
                });

        Aws::S3::Model::CompletedMultipartUpload completed_upload;
        completed_upload.WithParts(completed_parts);
        Aws::S3::Model::CompleteMultipartUploadRequest complete_request;
        complete_request.WithBucket(bucket_name)
                        .WithKey(object_key)
                        .WithUploadId(upload_id)
                        .WithMultipartUpload(completed_upload);

        auto complete_response = s3_client.CompleteMultipartUpload(complete_request);
        if (!complete_response.IsSuccess()) {
            std::cerr << "Failed to complete multipart upload: " 
                      << complete_response.GetError().GetMessage() << std::endl;
        } else {
            std::cout << "Multipart upload completed successfully!" << std::endl;
        }

        // 记录结束时间，并以毫秒为单位输出总上传时间
        auto end_time = std::chrono::high_resolution_clock::now();
        auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        std::cout << "Total upload time: " << elapsed_ms.count() << " ms" << std::endl;
    }
    Aws::ShutdownAPI(options);
}

int main() {
    const std::string bucket_name = "generalbuckets-jx";
    const std::string object_key = "file.bin";
    const size_t file_size_mb = 32;               // 512 MB
    const size_t part_size = 8 * 1024 * 1024;       // 32 MB per part
    const size_t thread_count = 4;                // 并发上传线程数

    try {
        std::cout << "Generating file in memory..." << std::endl;
        auto memory_file = GenerateMemoryFile(file_size_mb);

        std::cout << "Uploading memory file to S3..." << std::endl;
        UploadMemoryFileToS3(bucket_name, object_key, memory_file, part_size, thread_count);
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }

    return 0;
}
