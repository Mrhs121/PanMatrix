package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"panmatrix/config"
	"panmatrix/drivers"
	"panmatrix/metadata"
	"panmatrix/raid"
	"panmatrix/scheduler"
)

func main() {
	// 命令行参数
	raidLevel := flag.Int("raid", 0, "RAID级别 (0, 1, 5, 10)")
	uploadFile := flag.String("upload", "", "要上传的文件路径")
	downloadFile := flag.String("download", "", "要下载的文件ID")
	outputPath := flag.String("output", "./download", "下载文件输出路径")
	
	flag.Parse()
	
	// 加载配置
	cfg, err := config.LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}
	
	// 初始化存储驱动
	storageDrivers := initializeDrivers(cfg)
	if len(storageDrivers) < 2 {
		log.Fatal("至少需要2个存储驱动器")
	}
	
	// 初始化RAID控制器
	raidController, err := raid.NewRAIDController(
		raid.RAIDLevel(*raidLevel),
		storageDrivers,
		cfg.Core.ChunkSize,
	)
	if err != nil {
		log.Fatalf("初始化RAID控制器失败: %v", err)
	}
	
	// 初始化元数据管理器
	metaManager, err := metadata.NewMetadataManager(cfg.Core.MetadataPath)
	if err != nil {
		log.Fatalf("初始化元数据管理器失败: %v", err)
	}
	
	// 初始化调度器
	raidScheduler := scheduler.NewRAIDScheduler(storageDrivers)
	
	// 根据命令行参数执行操作
	ctx := context.Background()
	
	if *uploadFile != "" {
		if err := handleUpload(ctx, raidController, metaManager, raidScheduler, *uploadFile, *raidLevel); err != nil {
			log.Fatalf("上传失败: %v", err)
		}
	} else if *downloadFile != "" {
		if err := handleDownload(ctx, raidController, metaManager, *downloadFile, *outputPath); err != nil {
			log.Fatalf("下载失败: %v", err)
		}
	} else {
		// 启动交互式命令行或Web界面
		startInteractive(raidController, metaManager, raidScheduler)
	}
}

func initializeDrivers(cfg *config.Config) map[string]drivers.StorageDriver {
	driversMap := make(map[string]drivers.StorageDriver)
	
	// 百度网盘驱动
	if cfg.Baidu.Enabled {
		baiduDriver, err := drivers.NewBaiduDriver(cfg.Baidu)
		if err != nil {
			log.Printf("警告: 初始化百度驱动失败: %v", err)
		} else {
			driversMap["baidu"] = baiduDriver
			if err := baiduDriver.Connect(); err != nil {
				log.Printf("警告: 连接百度网盘失败: %v", err)
			}
		}
	}
	
	// 阿里云盘驱动
	if cfg.Aliyun.Enabled {
		aliyunDriver, err := drivers.NewAliyunDriver(cfg.Aliyun)
		if err != nil {
			log.Printf("警告: 初始化阿里云驱动失败: %v", err)
		} else {
			driversMap["aliyun"] = aliyunDriver
			if err := aliyunDriver.Connect(); err != nil {
				log.Printf("警告: 连接阿里云盘失败: %v", err)
			}
		}
	}
	
	// OneDrive驱动（示例，需要实现）
	// if cfg.OneDrive.Enabled {
	//     oneDriveDriver, err := drivers.NewOneDriveDriver(cfg.OneDrive)
	//     if err == nil {
	//         driversMap["onedrive"] = oneDriveDriver
	//     }
	// }
	
	// 本地缓存驱动（必须）
	localDriver, err := drivers.NewLocalDriver(cfg.Local)
	if err != nil {
		log.Fatalf("初始化本地驱动失败: %v", err)
	}
	driversMap["local"] = localDriver
	
	return driversMap
}

func handleUpload(ctx context.Context, rc *raid.RAIDController, mm *metadata.MetadataManager, 
	rs *scheduler.RAIDScheduler, filePath string, raidLevel int) error {
	
	// 读取文件
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("读取文件失败: %v", err)
	}
	
	fmt.Printf("开始上传文件: %s (大小: %.2f MB)\n", 
		filePath, float64(len(data))/(1024*1024))
	
	startTime := time.Now()
	
	// 使用RAID控制器写入文件
	fileID, err := rc.WriteFile(ctx, filePath, data)
	if err != nil {
		return fmt.Errorf("RAID写入失败: %v", err)
	}
	
	// 创建并保存元数据
	metadata := &metadata.FileMetadata{
		FileID:      fileID,
		FileName:    filePath,
		FileSize:    int64(len(data)),
		RAIDLevel:   raidLevel,
		StripeSize:  rc.StripeSize,
		StripeCount: int((int64(len(data)) + rc.StripeSize - 1) / rc.StripeSize),
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}
	
	if err := mm.SaveFileMetadata(metadata); err != nil {
		return fmt.Errorf("保存元数据失败: %v", err)
	}
	
	duration := time.Since(startTime)
	speed := float64(len(data)) / duration.Seconds() / (1024 * 1024) // MB/s
	
	fmt.Printf("上传成功! 文件ID: %s\n", fileID)
	fmt.Printf("耗时: %.2f秒, 平均速度: %.2f MB/s\n", duration.Seconds(), speed)
	fmt.Printf("RAID级别: %d, 条带大小: %d字节\n", raidLevel, rc.StripeSize)
	
	return nil
}

func handleDownload(ctx context.Context, rc *raid.RAIDController, mm *metadata.MetadataManager, 
	fileID, outputPath string) error {
	
	fmt.Printf("开始下载文件: %s\n", fileID)
	
	startTime := time.Now()
	
	// 使用RAID控制器读取文件
	data, err := rc.ReadFile(ctx, fileID)
	if err != nil {
		return fmt.Errorf("RAID读取失败: %v", err)
	}
	
	// 获取文件元数据以确定文件名
	meta, err := mm.GetFileMetadata(fileID)
	if err != nil {
		// 如果无法获取元数据，使用文件ID作为文件名
		outputPath = fmt.Sprintf("%s/%s.download", outputPath, fileID)
	} else {
		outputPath = fmt.Sprintf("%s/%s", outputPath, meta.FileName)
	}
	
	// 确保输出目录存在
	if err := os.MkdirAll(outputPath, 0755); err != nil {
		return fmt.Errorf("创建输出目录失败: %v", err)
	}
	
	// 写入文件
	if err := os.WriteFile(outputPath, data, 0644); err != nil {
		return fmt.Errorf("写入文件失败: %v", err)
	}
	
	duration := time.Since(startTime)
	speed := float64(len(data)) / duration.Seconds() / (1024 * 1024) // MB/s
	
	fmt.Printf("下载成功! 保存到: %s\n", outputPath)
	fmt.Printf("耗时: %.2f秒, 平均速度: %.2f MB/s\n", duration.Seconds(), speed)
	
	return nil
}

func startInteractive(rc *raid.RAIDController, mm *metadata.MetadataManager, rs *scheduler.RAIDScheduler) {
	fmt.Println("=== PanMatrix RAID-over-Cloud 系统 ===")
	fmt.Println("1. 上传文件")
	fmt.Println("2. 下载文件")
	fmt.Println("3. 列出文件")
	fmt.Println("4. 系统状态")
	fmt.Println("5. 退出")
	
	//Todo web 
	
	fmt.Println("交互式界面待实现...")
}
