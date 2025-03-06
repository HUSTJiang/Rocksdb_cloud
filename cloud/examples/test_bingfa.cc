#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <iostream>
#include <vector>
#include <thread>
#include <future>
#include <chrono>
#include <sstream>
#include <mutex>
#include <algorithm>

// 在内存中生成一个指定大小的数据（以 'A' 填充）
std::vector<char> GenerateMemoryFile(size_t size_in_mb) {
    size_t total_size = size_in_mb * 1024 * 1024;
    return std::vector<char>(total_size, 'A');
}

void UploadMemoryBufferToS3(const std::string& bucket_name, const std::string& object_key,
                              const std::vector<char>& buffer, size_t part_size, size_t thread_count) {
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    {
        Aws::S3::S3Client s3_client;

        // 记录上传开始时间
        auto start_time = std::chrono::high_resolution_clock::now();

        size_t total_size = buffer.size();
        size_t part_index = 0;
        size_t offset = 0;
        std::vector<std::future<void>> futures;
        std::mutex print_mutex;

        // 定义上传分段的lambda函数
        auto upload_part = [&](size_t index, const std::vector<char>& part_buffer) {
            // 为每个分段构造唯一的对象Key
            std::string part_key = object_key + "_part_" + std::to_string(index);

            Aws::S3::Model::PutObjectRequest request;
            request.WithBucket(bucket_name).WithKey(part_key);

            auto stream = Aws::MakeShared<Aws::StringStream>("PutObjectStream");
            stream->write(part_buffer.data(), part_buffer.size());
            request.SetBody(stream);

            auto outcome = s3_client.PutObject(request);
            if (outcome.IsSuccess()) {
                std::lock_guard<std::mutex> lock(print_mutex);
                std::cout << "Uploaded part " << index << " successfully." << std::endl;
            } else {
                throw std::runtime_error("Failed to upload part " + std::to_string(index) +
                                           ": " + outcome.GetError().GetMessage());
            }
        };

        // 分段上传
        while (offset < total_size) {
            size_t current_part_size = std::min(part_size, total_size - offset);
            std::vector<char> part_buffer(buffer.begin() + offset, buffer.begin() + offset + current_part_size);

            // 控制并发线程数
            if (futures.size() >= thread_count) {
                futures.front().get();
                futures.erase(futures.begin());
            }

            futures.push_back(std::async(std::launch::async, upload_part, part_index, part_buffer));
            part_index++;
            offset += current_part_size;
        }

        // 等待所有任务完成
        for (auto& future : futures) {
            future.get();
        }

        // 记录上传结束时间，并以毫秒计算持续时间
        auto end_time = std::chrono::high_resolution_clock::now();
        auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        std::cout << "Total upload time: " << elapsed_ms.count() << " ms" << std::endl;
    }
    Aws::ShutdownAPI(options);
}

int main() {
    const std::string bucket_name = "generalbuckets-jx";
    const std::string object_key = "file.bin";
    const size_t file_size_mb = 64;            // 512 MB
    const size_t part_size = 4 * 1024 * 1024;   // 分片大小 64 MB
    const size_t thread_count = 16;               // 并发上传线程数

    try {
        std::cout << "Generating file in memory..." << std::endl;
        auto memory_file = GenerateMemoryFile(file_size_mb);

        std::cout << "Uploading memory buffer to S3..." << std::endl;
        UploadMemoryBufferToS3(bucket_name, object_key, memory_file, part_size, thread_count);
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }

    return 0;
}
