#include <iostream>
#include <fstream>
#include <chrono>
#include <vector>
#include <cstring>
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/PutObjectRequest.h>

const size_t kFileSize = 8 * 1024 * 1024;  // 64MB
const char* kFileName = "testfile.bin";

// AWS配置（需根据实际情况修改）
const Aws::String s3_bucket = "generalbuckets-jx";
const Aws::String s3_object = "testfile.bin";

void measure_disk_write(const std::vector<char>& buffer) {
    // 创建并打开文件
    std::ofstream out_file(kFileName, std::ios::binary | std::ios::trunc);
    if (!out_file) {
        std::cerr << "无法打开文件: " << kFileName << std::endl;
        return ;
    }

    // 写入文件并计时
    auto start_time = std::chrono::high_resolution_clock::now();
    
    out_file.write(buffer.data(), buffer.size());
    if (!out_file) {
        std::cerr << "文件写入失败" << std::endl;
        return;
    }
    
    out_file.close(); // 确保数据完全写入
    
    auto end_time = std::chrono::high_resolution_clock::now();

    // 计算并显示耗时
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_time - start_time
    );
    
    std::cout << "成功写入 " << kFileSize / (1024 * 1024) << "MB 数据\n"
              << "耗时: " << duration.count() << " 毫秒\n"
              << "速度: " << (kFileSize / (1024.0 * 1024)) / (duration.count() / 1000.0) 
              << " MB/s" << std::endl;

    return;
}

void measure_s3_upload(const std::vector<char>& buffer) {
    Aws::Client::ClientConfiguration config;

    Aws::S3::S3Client s3_client(config);
    
    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(s3_bucket);
    request.SetKey(s3_object);

    // 创建内存数据流
    auto ss = Aws::MakeShared<Aws::StringStream>("s3-upload-stream");
    ss->write(buffer.data(), buffer.size());
    request.SetBody(ss);

    // 执行上传并计时
    auto start = std::chrono::high_resolution_clock::now();
    auto outcome = s3_client.PutObject(request);
    auto end = std::chrono::high_resolution_clock::now();

    if (!outcome.IsSuccess()) {
        std::cerr << "S3上传失败: " << outcome.GetError().GetMessage() << std::endl;
        return;
    }

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "\nS3上传耗时: " << duration.count() << " ms\n"
              << "上传速度: " 
              << (kFileSize/(1024.0*1024)) / (duration.count()/1000.0) 
              << " MB/s" << std::endl;
}

int main() {
    Aws::SDKOptions options;
    Aws::InitAPI(options);  // 初始化AWS SDK

    std::vector<char> buffer(kFileSize, 0);  // 创建内存缓冲区

    measure_disk_write(buffer);  // 原有磁盘写入测试
    measure_s3_upload(buffer);   // 新增S3上传测试

    Aws::ShutdownAPI(options);  // 清理AWS资源
    return 0;
}