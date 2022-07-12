package mr

import "time"

// worker type

const (
	MAPTYPE    = 0
	REDUCETYPE = 1
	EMPTY      = 2
)

// worker state
const (
	IDLE     = iota // 空闲
	RUNNING         // 正在做任务
	FINISHED        // 任务已完成
	CRASH           // 坏掉了
)

// worker max number
const (
	MAXNUM = 50
)

// 时间
const (
	MAXWAITTIME = time.Second * 10
)
