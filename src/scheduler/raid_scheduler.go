package scheduler

import (
	"context"
	"sort"
	"sync"
	"time"

	"panmatrix/drivers"
)

// 驱动器性能指标
type DriverMetrics struct {
	Name          string
	AvgLatency    time.Duration // 平均延迟
	SuccessRate   float64       // 成功率
	CurrentLoad   int           // 当前负载
	AvailableSpace int64        // 可用空间
	LastErrorTime time.Time     // 上次错误时间
}

// 智能RAID调度器
type RAIDScheduler struct {
	drivers    map[string]drivers.StorageDriver
	metrics    map[string]*DriverMetrics
	mu         sync.RWMutex
	
	// 调度策略
	preferLowLatency bool
	balanceLoad      bool
}

func NewRAIDScheduler(drivers map[string]drivers.StorageDriver) *RAIDScheduler {
	scheduler := &RAIDScheduler{
		drivers: drivers,
		metrics: make(map[string]*DriverMetrics),
		preferLowLatency: true,
		balanceLoad:      true,
	}
	
	// 初始化指标
	for name := range drivers {
		scheduler.metrics[name] = &DriverMetrics{
			Name:        name,
			AvgLatency:  100 * time.Millisecond, // 默认值
			SuccessRate: 1.0,
			CurrentLoad: 0,
		}
	}
	
	// 启动后台监控
	go scheduler.monitorDrivers()
	
	return scheduler
}

// 为RAID条带选择最优的驱动器组合
func (rs *RAIDScheduler) SelectDriversForStripe(raidLevel int, stripeIndex int, excludeDrivers []string) []string {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	
	// 获取所有可用的驱动器
	availableDrivers := rs.getAvailableDrivers(excludeDrivers)
	
	switch raidLevel {
	case 0: // RAID0
		return rs.selectForRAID0(availableDrivers)
	case 1: // RAID1
		return rs.selectForRAID1(availableDrivers)
	case 5: // RAID5
		return rs.selectForRAID5(availableDrivers, stripeIndex)
	case 10: // RAID10
		return rs.selectForRAID10(availableDrivers)
	default:
		return availableDrivers[:min(4, len(availableDrivers))] // 默认选择前4个
	}
}

// RAID0选择：性能优先
func (rs *RAIDScheduler) selectForRAID0(availableDrivers []string) []string {
	// 按性能排序（延迟低、成功率高）
	sorted := rs.sortDriversByPerformance(availableDrivers)
	
	// 选择前N个（N至少为2）
	count := min(4, len(sorted))
	return sorted[:count]
}

// RAID1选择：可靠性优先
func (rs *RAIDScheduler) selectForRAID1(availableDrivers []string) []string {
	// 按可靠性排序（成功率高、最近无错误）
	sorted := rs.sortDriversByReliability(availableDrivers)
	
	// 至少选择2个
	count := min(2, len(sorted))
	return sorted[:count]
}

// RAID5选择：考虑奇偶校验轮转
func (rs *RAIDScheduler) selectForRAID5(availableDrivers []string, stripeIndex int) []string {
	// 需要至少3个驱动器
	if len(availableDrivers) < 3 {
		return availableDrivers
	}
	
	// 按综合评分排序
	sorted := rs.sortDriversByScore(availableDrivers)
	
	// 选择前N个（N>=3）
	count := min(5, len(sorted))
	selected := sorted[:count]
	
	// 为当前条带确定奇偶校验驱动器
	parityIndex := stripeIndex % len(selected)
	
	// 将奇偶校验驱动器放到列表末尾（便于处理）
	if parityIndex < len(selected)-1 {
		selected[parityIndex], selected[len(selected)-1] = 
			selected[len(selected)-1], selected[parityIndex]
	}
	
	return selected
}

// RAID10选择：创建镜像对
func (rs *RAIDScheduler) selectForRAID10(availableDrivers []string) []string {
	if len(availableDrivers) < 4 {
		return availableDrivers
	}
	
	// 按可靠性分组，相似的驱动器组成镜像对
	sorted := rs.sortDriversByReliability(availableDrivers)
	
	// 取前偶数个
	count := min(8, len(sorted))
	if count%2 != 0 {
		count--
	}
	
	return sorted[:count]
}

// 根据性能排序
func (rs *RAIDScheduler) sortDriversByPerformance(drivers []string) []string {
	sort.Slice(drivers, func(i, j int) bool {
		mi := rs.metrics[drivers[i]]
		mj := rs.metrics[drivers[j]]
		
		// 比较延迟
		if mi.AvgLatency != mj.AvgLatency {
			return mi.AvgLatency < mj.AvgLatency
		}
		
		// 比较成功率
		return mi.SuccessRate > mj.SuccessRate
	})
	
	return drivers
}

// 根据可靠性排序
func (rs *RAIDScheduler) sortDriversByReliability(drivers []string) []string {
	sort.Slice(drivers, func(i, j int) bool {
		mi := rs.metrics[drivers[i]]
		mj := rs.metrics[drivers[j]]
		
		// 比较成功率
		if mi.SuccessRate != mj.SuccessRate {
			return mi.SuccessRate > mj.SuccessRate
		}
		
		// 比较最近错误时间（越久远越好）
		return mi.LastErrorTime.Before(mj.LastErrorTime)
	})
	
	return drivers
}

// 根据综合评分排序
func (rs *RAIDScheduler) sortDriversByScore(drivers []string) []string {
	sort.Slice(drivers, func(i, j int) bool {
		return rs.calculateScore(drivers[i]) > rs.calculateScore(drivers[j])
	})
	
	return drivers
}

// 计算驱动器综合评分
func (rs *RAIDScheduler) calculateScore(driverName string) float64 {
	metric := rs.metrics[driverName]
	if metric == nil {
		return 0
	}
	
	score := 0.0
	
	// 延迟评分（越低越好）
	latencyScore := 1.0 / (float64(metric.AvgLatency.Milliseconds()) + 1)
	score += latencyScore * 0.3
	
	// 成功率评分
	score += metric.SuccessRate * 0.4
	
	// 负载评分（负载越低越好）
	loadScore := 1.0 / (float64(metric.CurrentLoad) + 1)
	score += loadScore * 0.2
	
	// 空间评分（可用空间越多越好）
	if metric.AvailableSpace > 0 {
		spaceScore := float64(min(metric.AvailableSpace, 10*1024*1024*1024)) / (10 * 1024 * 1024 * 1024)
		score += spaceScore * 0.1
	}
	
	return score
}

// 获取可用的驱动器列表（排除不健康的）
func (rs *RAIDScheduler) getAvailableDrivers(excludeDrivers []string) []string {
	excludeMap := make(map[string]bool)
	for _, name := range excludeDrivers {
		excludeMap[name] = true
	}
	
	var available []string
	for name, metric := range rs.metrics {
		if excludeMap[name] {
			continue
		}
		
		// 检查驱动器健康状态
		if metric.SuccessRate > 0.8 && // 成功率高于80%
			time.Since(metric.LastErrorTime) > 5*time.Minute { // 5分钟内无错误
			available = append(available, name)
		}
	}
	
	return available
}

// 记录操作结果，更新指标
func (rs *RAIDScheduler) RecordOperation(driverName string, success bool, latency time.Duration) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	
	metric, exists := rs.metrics[driverName]
	if !exists {
		return
	}
	
	// 更新延迟（指数加权移动平均）
	if metric.AvgLatency == 0 {
		metric.AvgLatency = latency
	} else {
		alpha := 0.1 // 平滑因子
		metric.AvgLatency = time.Duration(
			float64(metric.AvgLatency)*(1-alpha) + float64(latency)*alpha,
		)
	}
	
	// 更新成功率
	totalOps := 100 // 假设跟踪最近100次操作
	successCount := int(metric.SuccessRate * float64(totalOps))
	if success {
		successCount = min(successCount+1, totalOps)
	} else {
		successCount = max(successCount-1, 0)
		metric.LastErrorTime = time.Now()
	}
	metric.SuccessRate = float64(successCount) / float64(totalOps)
	
	// 更新负载
	if success {
		metric.CurrentLoad = max(metric.CurrentLoad-1, 0)
	} else {
		metric.CurrentLoad = min(metric.CurrentLoad+1, 100)
	}
}

// 后台监控驱动器状态
func (rs *RAIDScheduler) monitorDrivers() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for range ticker.C {
		rs.checkDriverHealth()
	}
}

func (rs *RAIDScheduler) checkDriverHealth() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	for name, driver := range rs.drivers {
		start := time.Now()
		
		// 检查驱动器是否可用
		available := driver.IsAvailable()
		latency := time.Since(start)
		
		metric := rs.metrics[name]
		if metric == nil {
			metric = &DriverMetrics{Name: name}
			rs.metrics[name] = metric
		}
		
		if !available {
			metric.SuccessRate = max(metric.SuccessRate-0.1, 0)
			metric.LastErrorTime = time.Now()
		}
		
		// 获取空间信息
		used, total, err := driver.GetUsage()
		if err == nil {
			metric.AvailableSpace = total - used
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
