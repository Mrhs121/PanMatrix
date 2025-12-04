package metadata

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// 文件元数据
type FileMetadata struct {
	FileID      string                 `json:"file_id"`
	FileName    string                 `json:"file_name"`
	FileSize    int64                  `json:"file_size"`
	RAIDLevel   int                    `json:"raid_level"`
	StripeSize  int64                  `json:"stripe_size"`
	StripeCount int                    `json:"stripe_count"`
	CreatedAt   time.Time              `json:"created_at"`
	UpdatedAt   time.Time              `json:"updated_at"`
	Hash        string                 `json:"hash"`
	
	// RAID特定的元数据
	Stripes     []StripeMetadata       `json:"stripes"`
	DriverMap   map[string]DriverInfo  `json:"driver_map"` // 驱动器健康状态
}

// 条带元数据
type StripeMetadata struct {
	StripeIndex int                    `json:"stripe_index"`
	Strips      []StripMetadata        `json:"strips"`
	ParityStrip *StripMetadata         `json:"parity_strip,omitempty"` // RAID5
}

// 块元数据
type StripMetadata struct {
	StripIndex  int      `json:"strip_index"`
	DriverName  string   `json:"driver_name"`
	StorageID   string   `json:"storage_id"`
	StripSize   int64    `json:"strip_size"`
	IsParity    bool     `json:"is_parity"`
	Checksum    string   `json:"checksum"`
	CreatedAt   time.Time `json:"created_at"`
}

// 驱动器信息
type DriverInfo struct {
	Name        string    `json:"name"`
	Health      string    `json:"health"` // healthy, degraded, failed
	LastCheck   time.Time `json:"last_check"`
	UsedSpace   int64     `json:"used_space"`
	TotalSpace  int64     `json:"total_space"`
}

// 元数据管理器
type MetadataManager struct {
	basePath      string
	metadata      map[string]*FileMetadata
	driverHealth  map[string]*DriverInfo
	mu            sync.RWMutex
}

func NewMetadataManager(basePath string) (*MetadataManager, error) {
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, fmt.Errorf("创建元数据目录失败: %v", err)
	}
	
	mm := &MetadataManager{
		basePath:     basePath,
		metadata:     make(map[string]*FileMetadata),
		driverHealth: make(map[string]*DriverInfo),
	}
	
	// 加载已有的元数据
	if err := mm.loadMetadata(); err != nil {
		return nil, err
	}
	
	return mm, nil
}

// 保存文件元数据
func (mm *MetadataManager) SaveFileMetadata(fm *FileMetadata) error {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	
	fm.UpdatedAt = time.Now()
	mm.metadata[fm.FileID] = fm
	
	// 保存到文件
	filePath := filepath.Join(mm.basePath, fm.FileID+".json")
	data, err := json.MarshalIndent(fm, "", "  ")
	if err != nil {
		return fmt.Errorf("序列化元数据失败: %v", err)
	}
	
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("写入元数据文件失败: %v", err)
	}
	
	return nil
}

// 获取文件元数据
func (mm *MetadataManager) GetFileMetadata(fileID string) (*FileMetadata, error) {
	mm.mu.RLock()
	defer mm.mu.RUnlock()
	
	// 首先从内存缓存查找
	if fm, exists := mm.metadata[fileID]; exists {
		return fm, nil
	}
	
	// 从文件加载
	filePath := filepath.Join(mm.basePath, fileID+".json")
	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("文件不存在: %s", fileID)
		}
		return nil, fmt.Errorf("读取元数据失败: %v", err)
	}
	
	var fm FileMetadata
	if err := json.Unmarshal(data, &fm); err != nil {
		return nil, fmt.Errorf("解析元数据失败: %v", err)
	}
	
	// 缓存到内存
	mm.metadata[fileID] = &fm
	
	return &fm, nil
}

// 记录驱动器健康状态
func (mm *MetadataManager) UpdateDriverHealth(driverName, health string, usedSpace, totalSpace int64) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	
	mm.driverHealth[driverName] = &DriverInfo{
		Name:       driverName,
		Health:     health,
		LastCheck:  time.Now(),
		UsedSpace:  usedSpace,
		TotalSpace: totalSpace,
	}
}

// 获取不健康的驱动器列表
func (mm *MetadataManager) GetUnhealthyDrivers() []string {
	mm.mu.RLock()
	defer mm.mu.RUnlock()
	
	var unhealthy []string
	for name, info := range mm.driverHealth {
		if info.Health != "healthy" {
			unhealthy = append(unhealthy, name)
		}
	}
	
	return unhealthy
}

// 为RAID5记录奇偶校验分布
func (mm *MetadataManager) RecordParityDistribution(fileID string, stripeIndex, parityDriverIndex int) error {
	fm, err := mm.GetFileMetadata(fileID)
	if err != nil {
		return err
	}
	
	mm.mu.Lock()
	defer mm.mu.Unlock()
	
	// 确保有足够的条带
	for len(fm.Stripes) <= stripeIndex {
		fm.Stripes = append(fm.Stripes, StripeMetadata{
			StripeIndex: len(fm.Stripes),
			Strips:      make([]StripMetadata, 0),
		})
	}
	
	// 记录奇偶校验位置
	stripe := &fm.Stripes[stripeIndex]
	if stripe.ParityStrip == nil {
		stripe.ParityStrip = &StripMetadata{
			StripIndex: parityDriverIndex,
			IsParity:   true,
			CreatedAt:  time.Now(),
		}
	}
	
	return mm.SaveFileMetadata(fm)
}

// 加载所有元数据
func (mm *MetadataManager) loadMetadata() error {
	entries, err := os.ReadDir(mm.basePath)
	if err != nil {
		return err
	}
	
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".json" {
			filePath := filepath.Join(mm.basePath, entry.Name())
			data, err := os.ReadFile(filePath)
			if err != nil {
				fmt.Printf("警告: 无法读取元数据文件 %s: %v\n", filePath, err)
				continue
			}
			
			var fm FileMetadata
			if err := json.Unmarshal(data, &fm); err != nil {
				fmt.Printf("警告: 无法解析元数据文件 %s: %v\n", filePath, err)
				continue
			}
			
			mm.metadata[fm.FileID] = &fm
		}
	}
	
	return nil
}
