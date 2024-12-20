#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CompletedPart.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <fstream>
#include <iostream>
#include <vector>
#include <thread>
#include <future>
#include <chrono>
#include <algorithm> // 添加这个头文件以使用 std::sort

void GenerateFile(const std::string& file_path, size_t size_in_mb) {
    std::ofstream file(file_path, std::ios::binary);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to create file: " + file_path);
    }
    const size_t buffer_size = 1024 * 1024; // 1MB buffer
    std::vector<char> buffer(buffer_size, 'A');
    for (size_t i = 0; i < size_in_mb; ++i) {
        file.write(buffer.data(), buffer_size);
    }
    file.close();
}

void UploadFileToS3(const std::string& bucket_name, const std::string& object_key, 
                    const std::string& file_path, size_t part_size, size_t thread_count) {
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    {
        Aws::S3::S3Client s3_client;

        // Step 1: Initiate a multipart upload
        Aws::S3::Model::CreateMultipartUploadRequest create_request;
        create_request.WithBucket(bucket_name).WithKey(object_key);

        auto create_response = s3_client.CreateMultipartUpload(create_request);
        if (!create_response.IsSuccess()) {
            std::cerr << "Failed to initiate multipart upload: " 
                      << create_response.GetError().GetMessage() << std::endl;
            return;
        }
        std::string upload_id = create_response.GetResult().GetUploadId();

        // Start measuring time
        auto start_time = std::chrono::high_resolution_clock::now();

        // Step 2: Read the file and upload parts
        std::ifstream file(file_path, std::ios::binary);
        if (!file.is_open()) {
            throw std::runtime_error("Failed to open file: " + file_path);
        }

        std::vector<Aws::S3::Model::CompletedPart> completed_parts;
        std::mutex completed_parts_mutex;
        size_t part_number = 1;

        auto upload_part = [&](size_t part_number, const std::vector<char>& buffer) {
            Aws::S3::Model::UploadPartRequest part_request;
            part_request.WithBucket(bucket_name)
                        .WithKey(object_key)
                        .WithUploadId(upload_id)
                        .WithPartNumber(part_number);

            auto stream = Aws::MakeShared<Aws::StringStream>("UploadPartStream");
            stream->write(buffer.data(), buffer.size());
            part_request.SetBody(stream);
            part_request.SetContentLength(buffer.size());

            auto part_response = s3_client.UploadPart(part_request);
            if (part_response.IsSuccess()) {
                Aws::S3::Model::CompletedPart completed_part;
                completed_part.WithPartNumber(part_number)
                              .WithETag(part_response.GetResult().GetETag());
                std::lock_guard<std::mutex> lock(completed_parts_mutex);
                completed_parts.push_back(completed_part);
            } else {
                throw std::runtime_error("Failed to upload part: " 
                                         + part_response.GetError().GetMessage());
            }
        };

        std::vector<std::future<void>> futures;
        std::vector<char> buffer(part_size);

        while (file.read(buffer.data(), part_size) || file.gcount() > 0) {
            size_t bytes_read = file.gcount();
            buffer.resize(bytes_read);

            if (futures.size() >= thread_count) {
                futures.front().get();
                futures.erase(futures.begin());
            }

            futures.push_back(std::async(std::launch::async, upload_part, part_number, buffer));
            part_number++;
            buffer.resize(part_size);
        }

        for (auto& future : futures) {
            future.get();
        }
        file.close();

        // Step 3: Complete the multipart upload
        Aws::S3::Model::CompleteMultipartUploadRequest complete_request;
        complete_request.WithBucket(bucket_name)
                        .WithKey(object_key)
                        .WithUploadId(upload_id);

        // 在完成上传前，对 completed_parts 按 PartNumber 升序排序
        std::sort(completed_parts.begin(), completed_parts.end(), 
                [](const Aws::S3::Model::CompletedPart& a, const Aws::S3::Model::CompletedPart& b) {
                    return a.GetPartNumber() < b.GetPartNumber();
                });

        Aws::S3::Model::CompletedMultipartUpload completed_upload;
        completed_upload.WithParts(completed_parts);
        complete_request.WithMultipartUpload(completed_upload);

        auto complete_response = s3_client.CompleteMultipartUpload(complete_request);
        if (!complete_response.IsSuccess()) {
            std::cerr << "Failed to complete multipart upload: " 
                      << complete_response.GetError().GetMessage() << std::endl;
        } else {
            std::cout << "Multipart upload completed successfully!" << std::endl;
        }

        // Stop measuring time
        auto end_time = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> elapsed_time = end_time - start_time;

        std::cout << "Total upload time: " << elapsed_time.count() << " seconds" << std::endl;
    }
    Aws::ShutdownAPI(options);
}

int main() {
    const std::string bucket_name = "generalbuckets-jx";
    const std::string object_key = "512MB_file.bin";
    const std::string file_path = "512MB_file.bin";
    const size_t file_size_mb = 512;   // 512 MB
    const size_t part_size = 32 * 1024 * 1024; // 5 MB
    const size_t thread_count = 16;    // 4 concurrent threads

    try {
        std::cout << "Generating file..." << std::endl;
        GenerateFile(file_path, file_size_mb);

        std::cout << "Uploading file to S3..." << std::endl;
        UploadFileToS3(bucket_name, object_key, file_path, part_size, thread_count);
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }

    return 0;
}
