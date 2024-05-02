> 注意:整章的源码在Spark源码关联

# 第一章 Spark 内核概述

Spark 内核泛指 Spark 的核心运行机制，包括 Spark 核心组件的运行机制、Spark 任务调 度机制、Spark 内存管理机制、Spark 核心功能的运行原理等，熟练掌握 Spark 内核原理，能 够帮助我们更好地完成 Spark 代码设计，并能够帮助我们准确锁定项目运行过程中出现的问 题的症结所在

##  Spark 核心组件回顾

### Driver

Spark 驱动器节点，用于执行 Spark 任务中的 main 方法，负责实际代码的执行工作。 Driver 在 Spark 作业执行时主要负责

1. 将用户程序转化为作业（Job）；
2. 在 Executor 之间调度任务（Task）；
3. 跟踪 Executor 的执行情况；
4. 通过 UI 展示查询运行情况

### Executor

Spark Executor 对象是负责在 Spark 作业中运行具体任务，任务彼此之间相互独立。Spark 应用启动时，ExecutorBackend 节点被同时启动，并且始终伴随着整个 Spark 应用的生命周 期而存在。如果有 ExecutorBackend 节点发生了故障或崩溃，Spark 应用也可以继续执行， 会将出错节点上的任务调度到其他 Executor 节点上继续运行

Executor 有两个核心功能：

1. 负责运行组成 Spark 应用的任务，并将结果返回给驱动器（Driver）；
2. 它们通过自身的块管理器（Block Manager）为用户程序中要求缓存的 RDD 提供内存 式存储。RDD 是直接缓存在 Executor 进程内的，因此任务可以在运行时充分利用缓存 数据加速运算

## Spark 通用运行流程概述

> 图中的反向注册对应Spark源码中环境准备图的第7步 注册Executor

![](../photos/spark/21.jpg)

