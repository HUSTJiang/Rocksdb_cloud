#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <fstream>
#include <iostream>
#include <vector>
#include <thread>
#include <future>
#include <chrono>

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

        // Start measuring time
        auto start_time = std::chrono::high_resolution_clock::now();

        std::ifstream file(file_path, std::ios::binary);
        if (!file.is_open()) {
            throw std::runtime_error("Failed to open file: " + file_path);
        }

        std::vector<std::future<void>> futures;
        std::mutex print_mutex;
        size_t part_index = 0;

        auto upload_part = [&](size_t index, const std::vector<char>& buffer) {
            std::string part_key = object_key + "_part_" + std::to_string(index);

            Aws::S3::Model::PutObjectRequest request;
            request.WithBucket(bucket_name).WithKey(part_key);

            auto stream = Aws::MakeShared<Aws::StringStream>("PutObjectStream");
            stream->write(buffer.data(), buffer.size());
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

        std::vector<char> buffer(part_size);
        while (file.read(buffer.data(), part_size) || file.gcount() > 0) {
            size_t bytes_read = file.gcount();
            buffer.resize(bytes_read);

            if (futures.size() >= thread_count) {
                futures.front().get();
                futures.erase(futures.begin());
            }

            futures.push_back(std::async(std::launch::async, upload_part, part_index, buffer));
            part_index++;
            buffer.resize(part_size);
        }

        for (auto& future : futures) {
            future.get();
        }
        file.close();

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
    const size_t part_size = 64 * 1024 * 1024; // 5 MB
    const size_t thread_count = 8;    // 4 concurrent threads

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
