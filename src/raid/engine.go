package raid

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"

	"drivers"
)

// RAID级别定义
type RAIDLevel int

const (
	RAID0  RAIDLevel = 0  // 条带化，无冗余
	RAID1  RAIDLevel = 1  // 镜像
	RAID5  RAIDLevel = 5  // 分布式奇偶校验
	RAID10 RAIDLevel = 10 // 条带化+镜像
)

// Stripe: RAID中的条带概念
type Stripe struct {
	ID        string
	Size      int64
	DataStrip []*Strip  // 数据条带
	ParityStrip *Strip  // 奇偶校验条带（RAID5）
}

// Strip: 条带中的数据块
type Strip struct {
	Index      int            // 在条带中的索引
	Data       []byte         // 数据
	Location   StripLocation  // 存储位置
	IsParity   bool           // 是否为校验块
}

type StripLocation struct {
	DriverName string  // 驱动名称
	RemoteID   string  // 在远程存储中的ID
	Offset     int64   // 在文件中的偏移（用于大文件分块存储）
}

// RAID控制器
type RAIDController struct {
	level       RAIDLevel
	drivers     map[string]drivers.StorageDriver
	stripeSize  int64  // 条带大小（字节）
	stripeWidth int    // 条带宽度（驱动器数量）
	
	// 对于RAID5，需要记录奇偶校验分布
	parityRotation int  // 奇偶校验轮转
	
	mu sync.RWMutex
}

func NewRAIDController(level RAIDLevel, drivers map[string]drivers.StorageDriver, stripeSize int64) (*RAIDController, error) {
	driverCount := len(drivers)
	
	// 验证RAID级别与驱动器数量的兼容性
	switch level {
	case RAID0:
		if driverCount < 2 {
			return nil, errors.New("RAID0需要至少2个驱动器")
		}
	case RAID1:
		if driverCount < 2 {
			return nil, errors.New("RAID1需要至少2个驱动器")
		}
	case RAID5:
		if driverCount < 3 {
			return nil, errors.New("RAID5需要至少3个驱动器")
		}
	case RAID10:
		if driverCount < 4 || driverCount%2 != 0 {
			return nil, errors.New("RAID10需要至少4个且为偶数的驱动器")
		}
	default:
		return nil, errors.New("不支持的RAID级别")
	}
	
	return &RAIDController{
		level:       level,
		drivers:     drivers,
		stripeSize:  stripeSize,
		stripeWidth: driverCount,
	}, nil
}

// 写入文件，应用RAID策略
func (rc *RAIDController) WriteFile(ctx context.Context, fileName string, data []byte) (string, error) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	
	fileID := generateFileID(fileName)
	fileSize := int64(len(data))
	
	// 计算需要的条带数
	stripeCount := int(math.Ceil(float64(fileSize) / float64(rc.stripeSize)))
	
	// 为每个条带创建存储任务
	for stripeIndex := 0; stripeIndex < stripeCount; stripeIndex++ {
		// 计算当前条带的数据范围
		start := int64(stripeIndex) * rc.stripeSize
		end := start + rc.stripeSize
		if end > fileSize {
			end = fileSize
		}
		
		stripeData := data[start:end]
		
		// 根据RAID级别处理条带
		switch rc.level {
		case RAID0:
			if err := rc.writeRAID0Stripe(ctx, stripeIndex, stripeData, fileID); err != nil {
				return "", fmt.Errorf("写入RAID0条带失败: %v", err)
			}
		case RAID1:
			if err := rc.writeRAID1Stripe(ctx, stripeIndex, stripeData, fileID); err != nil {
				return "", fmt.Errorf("写入RAID1条带失败: %v", err)
			}
		case RAID5:
			if err := rc.writeRAID5Stripe(ctx, stripeIndex, stripeData, fileID); err != nil {
				return "", fmt.Errorf("写入RAID5条带失败: %v", err)
			}
		case RAID10:
			if err := rc.writeRAID10Stripe(ctx, stripeIndex, stripeData, fileID); err != nil {
				return "", fmt.Errorf("写入RAID10条带失败: %v", err)
			}
		}
	}
	
	return fileID, nil
}

// 读取文件，根据RAID策略重建数据
func (rc *RAIDController) ReadFile(ctx context.Context, fileID string) ([]byte, error) {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	
	// 首先尝试从元数据中获取文件信息（这里简化处理）
	// 在实际实现中，需要从元数据库查询条带分布
	
	// 模拟：假设我们知道文件由2个条带组成
	stripeCount := 2
	var fullData []byte
	
	for stripeIndex := 0; stripeIndex < stripeCount; stripeIndex++ {
		var stripeData []byte
		var err error
		
		switch rc.level {
		case RAID0:
			stripeData, err = rc.readRAID0Stripe(ctx, stripeIndex, fileID)
		case RAID1:
			stripeData, err = rc.readRAID1Stripe(ctx, stripeIndex, fileID)
		case RAID5:
			stripeData, err = rc.readRAID5Stripe(ctx, stripeIndex, fileID)
		case RAID10:
			stripeData, err = rc.readRAID10Stripe(ctx, stripeIndex, fileID)
		}
		
		if err != nil {
			return nil, fmt.Errorf("读取条带%d失败: %v", stripeIndex, err)
		}
		
		fullData = append(fullData, stripeData...)
	}
	
	return fullData, nil
}

// RAID0: 条带化写入
func (rc *RAIDController) writeRAID0Stripe(ctx context.Context, stripeIndex int, data []byte, fileID string) error {
	dataLen := len(data)
	stripSize := int(math.Ceil(float64(dataLen) / float64(rc.stripeWidth)))
	
	var wg sync.WaitGroup
	errCh := make(chan error, rc.stripeWidth)
	
	for i := 0; i < rc.stripeWidth; i++ {
		wg.Add(1)
		go func(stripIndex int) {
			defer wg.Done()
			
			start := stripIndex * stripSize
			end := start + stripSize
			if end > dataLen {
				end = dataLen
			}
			if start >= dataLen {
				return // 没有数据可写
			}
			
			stripData := data[start:end]
			
			// 选择驱动器
			driverName := rc.selectDriverForStrip(stripeIndex, stripIndex)
			driver := rc.drivers[driverName]
			
			// 构建唯一的存储ID
			storageID := fmt.Sprintf("%s_s%d_st%d", fileID, stripeIndex, stripIndex)
			
			_, err := driver.UploadChunk(ctx, stripData, storageID)
			if err != nil {
				errCh <- fmt.Errorf("驱动器%s写入失败: %v", driverName, err)
				return
			}
			
			// 记录元数据：fileID -> [条带1:[驱动器A,块1], [驱动器B,块2], ...]
			rc.recordMetadata(fileID, stripeIndex, stripIndex, driverName, storageID)
		}(i)
	}
	
	wg.Wait()
	close(errCh)
	
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	
	return nil
}

// RAID1: 镜像写入
func (rc *RAIDController) writeRAID1Stripe(ctx context.Context, stripeIndex int, data []byte, fileID string) error {
	// 将相同数据写入所有驱动器
	var wg sync.WaitGroup
	errCh := make(chan error, len(rc.drivers))
	
	for driverName, driver := range rc.drivers {
		wg.Add(1)
		go func(name string, drv drivers.StorageDriver) {
			defer wg.Done()
			
			storageID := fmt.Sprintf("%s_s%d_%s", fileID, stripeIndex, name)
			_, err := drv.UploadChunk(ctx, data, storageID)
			if err != nil {
				errCh <- fmt.Errorf("驱动器%s镜像写入失败: %v", name, err)
			}
		}(driverName, driver)
	}
	
	wg.Wait()
	close(errCh)
	
	// 只要有一个驱动器写入成功，就认为是成功的
	successCount := len(rc.drivers) - len(errCh)
	if successCount == 0 {
		return errors.New("所有驱动器写入失败")
	}
	
	return nil
}

// RAID5: 带分布式奇偶校验的条带化
func (rc *RAIDController) writeRAID5Stripe(ctx context.Context, stripeIndex int, data []byte, fileID string) error {
	// 将数据分成N-1块（N为驱动器数量）
	dataStrips := rc.splitDataForRAID5(data)
	
	// 计算奇偶校验
	parityStrip := rc.calculateParity(dataStrips)
	
	// 确定本轮奇偶校验存储的位置
	parityDriverIndex := stripeIndex % rc.stripeWidth
	
	var wg sync.WaitGroup
	errCh := make(chan error, rc.stripeWidth)
	
	for i := 0; i < rc.stripeWidth; i++ {
		wg.Add(1)
		go func(stripIndex int) {
			defer wg.Done()
			
			var stripData []byte
			var stripType string
			
			if stripIndex == parityDriverIndex {
				// 存储奇偶校验
				stripData = parityStrip
				stripType = "parity"
			} else {
				// 存储数据
				dataIndex := stripIndex
				if stripIndex > parityDriverIndex {
					dataIndex--
				}
				if dataIndex < len(dataStrips) {
					stripData = dataStrips[dataIndex]
				}
				stripType = "data"
			}
			
			if len(stripData) == 0 {
				return // 空数据块
			}
			
			driverName := rc.selectDriverByIndex(stripIndex)
			driver := rc.drivers[driverName]
			
			storageID := fmt.Sprintf("%s_s%d_%s_%s", fileID, stripeIndex, stripType, driverName)
			_, err := driver.UploadChunk(ctx, stripData, storageID)
			if err != nil {
				errCh <- fmt.Errorf("RAID5写入失败[%s]: %v", driverName, err)
			}
		}(i)
	}
	
	wg.Wait()
	close(errCh)
	
	// 允许一个驱动器失败（因为有奇偶校验）
	if len(errCh) > 1 {
		return errors.New("多个驱动器写入失败，RAID5无法保证数据安全")
	}
	
	return nil
}

// RAID10: 先镜像再条带化
func (rc *RAIDController) writeRAID10Stripe(ctx context.Context, stripeIndex int, data []byte, fileID string) error {
	// 将驱动器分成镜像对
	mirrorPairs := rc.createMirrorPairs()
	
	// 将数据条带化到每个镜像对
	stripsPerPair := len(mirrorPairs)
	stripSize := len(data) / stripsPerPair
	
	var wg sync.WaitGroup
	errCh := make(chan error, stripsPerPair*2) // 每个条带写入2个副本
	
	for pairIndex, pair := range mirrorPairs {
		wg.Add(2)
		
		// 计算当前镜像对的数据
		start := pairIndex * stripSize
		end := start + stripSize
		if pairIndex == stripsPerPair-1 {
			end = len(data) // 最后一个获取剩余所有数据
		}
		pairData := data[start:end]
		
		// 写入镜像对的两个驱动器
		for _, driverName := range pair {
			go func(name string, data []byte) {
				defer wg.Done()
				
				driver := rc.drivers[name]
				storageID := fmt.Sprintf("%s_s%d_pair%d_%s", fileID, stripeIndex, pairIndex, name)
				
				_, err := driver.UploadChunk(ctx, data, storageID)
				if err != nil {
					errCh <- fmt.Errorf("RAID10镜像对写入失败[%s]: %v", name, err)
				}
			}(driverName, pairData)
		}
	}
	
	wg.Wait()
	close(errCh)
	
	// 只要每个镜像对至少有一个副本成功即可
	if len(errCh) > stripsPerPair {
		return errors.New("太多镜像对写入失败")
	}
	
	return nil
}

// 读取RAID0条带
func (rc *RAIDController) readRAID0Stripe(ctx context.Context, stripeIndex int, fileID string) ([]byte, error) {
	// 从元数据获取条带分布（这里简化处理）
	// 实际应从元数据库查询
	
	var wg sync.WaitGroup
	strips := make([][]byte, rc.stripeWidth)
	errCh := make(chan error, rc.stripeWidth)
	
	for i := 0; i < rc.stripeWidth; i++ {
		wg.Add(1)
		go func(stripIndex int) {
			defer wg.Done()
			
			// 模拟：从元数据获取驱动器信息
			driverName := rc.selectDriverForStrip(stripeIndex, stripIndex)
			driver := rc.drivers[driverName]
			
			storageID := fmt.Sprintf("%s_s%d_st%d", fileID, stripeIndex, stripIndex)
			data, err := driver.DownloadChunk(ctx, storageID)
			if err != nil {
				errCh <- fmt.Errorf("读取条带块失败: %v", err)
				return
			}
			
			strips[stripIndex] = data
		}(i)
	}
	
	wg.Wait()
	close(errCh)
	
	// 检查错误
	if len(errCh) > 0 {
		return nil, errors.New("读取条带时发生错误")
	}
	
	// 合并所有数据块
	var result []byte
	for _, strip := range strips {
		if strip != nil {
			result = append(result, strip...)
		}
	}
	
	return result, nil
}

// 读取RAID5条带（带错误恢复）
func (rc *RAIDController) readRAID5Stripe(ctx context.Context, stripeIndex int, fileID string) ([]byte, error) {
	// 尝试读取所有块
	strips := make([][]byte, rc.stripeWidth)
	failedDrivers := make([]int, 0)
	
	var wg sync.WaitGroup
	mu := sync.Mutex{}
	
	for i := 0; i < rc.stripeWidth; i++ {
		wg.Add(1)
		go func(stripIndex int) {
			defer wg.Done()
			
			driverName := rc.selectDriverByIndex(stripIndex)
			driver := rc.drivers[driverName]
			
			// 尝试判断是数据块还是校验块
			parityDriverIndex := stripeIndex % rc.stripeWidth
			var stripType string
			if stripIndex == parityDriverIndex {
				stripType = "parity"
			} else {
				stripType = "data"
			}
			
			storageID := fmt.Sprintf("%s_s%d_%s_%s", fileID, stripeIndex, stripType, driverName)
			data, err := driver.DownloadChunk(ctx, storageID)
			
			mu.Lock()
			defer mu.Unlock()
			
			if err != nil {
				failedDrivers = append(failedDrivers, stripIndex)
			} else {
				strips[stripIndex] = data
			}
		}(i)
	}
	
	wg.Wait()
	
	// 检查是否需要恢复
	if len(failedDrivers) == 0 {
		// 所有块都成功读取，直接合并数据块
		return rc.mergeRAID5Strips(strips, stripeIndex), nil
	} else if len(failedDrivers) == 1 {
		// 一个块失败，使用奇偶校验恢复
		return rc.recoverRAID5Stripe(strips, failedDrivers[0], stripeIndex)
	} else {
		// 多个块失败，无法恢复
		return nil, errors.New("多个数据块丢失，无法恢复")
	}
}

// 辅助方法
func (rc *RAIDController) splitDataForRAID5(data []byte) [][]byte {
	// 将数据分成N-1块（N为驱动器数量）
	strips := make([][]byte, rc.stripeWidth-1)
	stripSize := len(data) / (rc.stripeWidth - 1)
	
	for i := 0; i < rc.stripeWidth-1; i++ {
		start := i * stripSize
		end := start + stripSize
		if i == rc.stripeWidth-2 {
			end = len(data) // 最后一个获取剩余所有
		}
		strips[i] = data[start:end]
	}
	
	return strips
}

func (rc *RAIDController) calculateParity(strips [][]byte) []byte {
	// 简单的异或奇偶校验计算
	if len(strips) == 0 {
		return []byte{}
	}
	
	// 找到最大长度
	maxLen := 0
	for _, strip := range strips {
		if len(strip) > maxLen {
			maxLen = len(strip)
		}
	}
	
	parity := make([]byte, maxLen)
	
	for _, strip := range strips {
		for i := 0; i < len(strip); i++ {
			parity[i] ^= strip[i]
		}
	}
	
	return parity
}

func (rc *RAIDController) selectDriverForStrip(stripeIndex, stripIndex int) string {
	// 简单的轮询选择
	driverNames := make([]string, 0, len(rc.drivers))
	for name := range rc.drivers {
		driverNames = append(driverNames, name)
	}
	
	totalIndex := stripeIndex*rc.stripeWidth + stripIndex
	return driverNames[totalIndex%len(driverNames)]
}

func (rc *RAIDController) selectDriverByIndex(index int) string {
	driverNames := make([]string, 0, len(rc.drivers))
	for name := range rc.drivers {
		driverNames = append(driverNames, name)
	}
	
	if index >= len(driverNames) {
		index = index % len(driverNames)
	}
	
	return driverNames[index]
}

func (rc *RAIDController) createMirrorPairs() [][]string {
	driverNames := make([]string, 0, len(rc.drivers))
	for name := range rc.drivers {
		driverNames = append(driverNames, name)
	}
	
	pairs := make([][]string, 0)
	for i := 0; i < len(driverNames); i += 2 {
		if i+1 < len(driverNames) {
			pairs = append(pairs, []string{driverNames[i], driverNames[i+1]})
		}
	}
	
	return pairs
}

func (rc *RAIDController) mergeRAID5Strips(strips [][]byte, stripeIndex int) []byte {
	parityIndex := stripeIndex % len(strips)
	var result []byte
	
	for i, strip := range strips {
		if i != parityIndex && strip != nil {
			result = append(result, strip...)
		}
	}
	
	return result
}

func (rc *RAIDController) recoverRAID5Stripe(strips [][]byte, failedIndex int, stripeIndex int) ([]byte, error) {
	// 使用奇偶校验和其他数据块恢复失败的数据块
	parityIndex := stripeIndex % len(strips)
	
	if failedIndex == parityIndex {
		// 奇偶校验块丢失，不影响数据读取
		return rc.mergeRAID5Strips(strips, stripeIndex), nil
	}
	
	// 数据块丢失，需要恢复
	// 这里简化处理，实际需要重新计算
	return nil, errors.New("数据块恢复功能待实现")
}

func (rc *RAIDController) recordMetadata(fileID string, stripeIndex, stripIndex int, driverName, storageID string) {
	// 在实际实现中，这里应该将元数据保存到数据库
	// 简化处理：打印日志
	fmt.Printf("记录元数据: 文件%s, 条带%d, 块%d -> %s:%s\n", 
		fileID, stripeIndex, stripIndex, driverName, storageID)
}

func generateFileID(fileName string) string {
	// 简单的文件ID生成
	return fmt.Sprintf("file_%s_%d", fileName, time.Now().UnixNano())
}
