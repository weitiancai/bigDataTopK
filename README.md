# 海量数据查询
### 内容：

实现SelectEngine的preSelect和bigDataSelect方法，回答多次bigDataSelect查询。


### 数据说明:
CSV格式数据文件,存在多个(文件名均以datafile开头，每个文件约为600M左右)，第一行均为列名，3列均为BIGINT且只有正数，单列无重复，数据量一共5亿行(不包括列头)，工程中的数据文件为仅仅作为测试使用

C1,C2,C3

138921890321889210,3218380219098057887,9889656755676878998

7678980801232132132,123213213213128998,6778643260889020021

### 题目说明：

参赛选手自主设计 preSelect(String dataFileDir) 数据预处理， bigDataSelect(String columnName,int k)返回指定列名升序排第K名所在的整行数据，以上面数据为例：

bigDataSelect("C1",1)需要返回 "138921890321889210,3218380219098057887,9889656755676878998"

bigDataSelect("C2",1)需要返回 "7678980801232132132,123213213213128998,6778643260889020021"

### 规则： 

1. 开发语言java，只能使用jdk8标准库 JVM参数 -Xmx4096m
2. 不允许硬编码返回结果;
3. 不要修改pom和SelectDriver文件;
4. 程序运行过程中如果需要产生新文件统一写入/user/syjnh/target,该目录程序启动前会自动清空;
5. 评测过程先执行一次preSelect然后单线程执行bigDataSelect查询60次;
6. 结果正确前提下按耗时排名,（耗时包括预加载数据时间和数据查询时间，选手自己设计数据预加载方式)，另外对获奖选手代码做二判，
   二判不影响统一评测成绩，只验证能否在不同数据集下跑出正确结果。

### 流程：

1. 组委会提供代码执行平台，平台硬件4核8G 300G硬盘，参赛者登录报名提交代码地址参赛，为保证公平代码执行平台同时只执行1份代码，多份提交需排队等待，等待时间不影响成绩。
2. 代码提交执行截止时间2022年9月6日24时，时间截止后系统会开始评测选手提交的代码，所以要求选手时间截止后不再修改代码,截止时间后还有提交记录的视为无效。
3. 执行时间超过30分钟视为超时会被系统终止无成绩，最终公布获胜者代码。


# 1、数据预处理——**分桶**
大数据量的处理，只有提高并行度、减少操作数据量 这个思路才能更快
这个过程有两部，一步是preSelect 第二步就是 bigDataSelect 
在预处理的过程中，需要对数据进行处理，一遍第二部数据查询
由于只是求单个 排序为k 的数据，所以并不需要全局排序
很自然的 可以想到，按分桶做，比如 将 1 开头的数字 分为一个桶（文件） 将2开头、、、 1~9 开头的文件可以分成这么多个桶
再根据数据量大小  改变数据前缀位数进行分桶的依据 比如 10~99 、 100~999 之类
但是这个方法的问题在于没有办法很好的 分隔全部数据，比如 10~99 的前面还有 0-9 这9个数要单独判断(或者默认就到0 的分桶上），
而且 不同长度的 十进制数 虽然前缀相同，但是 20 肯定 <100 的 ，所以在分桶时，还要按照长度设置一级分桶、在按照前缀进行二级分桶，这样很麻烦
原理： 因为我们需要到时候算 topK 的时候，每个桶内数据范围 按顺序 减去肯定不满足的那些桶内元素的数量，这样 topK 这个数肯定会存在于一个桶中
按十进制计算并不是计算速度最优解，
最优解肯定是2进制 结合位运算，： 按数据的2进制 最左边的几位进行分桶

原理其实就是 和十进制一样
如果按照 前2位二进制 分桶的话

> * 1001 1011   分到 10 的桶  即2
> * 1101 1111   分到 11 的桶  即3
> * 0001 0010   分到 00 的桶  即0
> * 0101 0110   分到 01 的桶  即1
因为可以用  数据 x>>2（正数可以无符号右移） 位的方式 分别得到10，11，0，1 这样的值， 刚好 对应的数据顺序肯定也是 0开头<1开头<10开头<11 开头

而**真正**在对5亿数据进行分桶时，是右移了7位，每个文件的数据都分成了128份
原因： long类型是占用8个Byte的，如果我们取第一个Byte的数字当做分桶依据的话，就能分128个桶 ，这是综合考虑下来的结果
比如下面代码就在做这个事情
`[DataLoadTask.java]中的 writersC1[writeBuffer[0]].write(writeBuffer);
`
还有就是，真正比赛数据，原本是一个大的文件，但是为了方便，拆成了5个文件（像大数据表中的分区）
这样的话，我们可以并行的用五个线程分别提取5个文件的数据
`public static final ExecutorService executor = Executors.newFixedThreadPool(THREAD_NUM);`

避免线程之间同时写入一个文件，肯定会有并发问题但是 用到了 BufferedOutputStream 写入，这中带Buffer的都是自带线程安全的？ 结合我之前代码
因为本质 BufferedOutputStream 中的 write  byte 数据带了 synchronized
`public synchronized void write(byte b[], int off, int len)`

这里有个注意点就是 ，写入byte 而不是写入数字、字符串之类的，虽然这样写入之后，认识没法看了，但是速度非常之快

另外读取写实不能通过平常的 ？ 
需要用到 mmap的技术，本质是 不走内核态（堆外内存），直接在用户态（jvm）映射一个内存地址给Java用  这样也能大大提升访问数据速度
`FileChannel fileChannel = new FileInputStream(dataFileList.get(data.getDataFileNum())).getChannel();
MappedByteBuffer mappedByteBuffer = fileChannel.map(MapMode.READ_ONLY, 0, fileChannel.size());`




> 因为查 某列数据后，需要返回的是整行的数据，所以，在分桶中写入那列值还是不够的，还需要记录位置（当然只需要行首的位置）
读取时，是几个G的文件，所以按照
`MappedByteBuffer mappedByteBuffer = fileChannel.map(MapMode.READ_ONLY, 0, fileChannel.size()); 返回一个MappedByteBuffer 其中
> 在每次读取一个ASCII码时 位置也会+1 最后每次碰到换行符之后 可以把位置记下来
readPosition = mappedByteBuffer.position();

这里其实有个问题 fileChannel.size() 是long类型 
返回一个MappedByteBuffer的position为int? 这样读取超过int长度的文件时，怎么得到position呢？  

`MappedByteBuffer mappedByteBuffer = fileChannel.map(MapMode.READ_ONLY, 0, fileChannel.size()) 为什么mappedByteBuffer 中的postition 是 int 类型的，因为 fileChannel.size()是long类型的，这是不是会有问题呢，读取了一个long长度的文件，但是mappedByteBuffer的position又只能记录int长度

您提出的问题很有意思。实际上，MappedByteBuffer中的position是可以记录超过int范围的偏移量的。尽管position字段声明为int类型，但Java NIO库中的操作会考虑到超过int范围的偏移量。

在您的示例代码中，MappedByteBuffer实例的位置position字段记录的是已映射文件中当前读取位置的字节偏移量。这个偏移量可以是一个long类型的值，因为文件大小可以超过int类型的最大值。在内部实现时，Java NIO库使用long类型来表示实际的偏移量，而不是使用int类型。

因此，尽管MappedByteBuffer中的position字段声明为int类型，但它实际上可以处理超过int范围的偏移量，因为Java NIO库内部使用long类型来表示当前位置。`

假设上述最大就是4个byte 所以可以用4个字节存储每行位置，这样，存储所用到的就是 8byte数字+4byte位置
具体还是看 


``当`MappedByteBuffer`中的`position`字段达到`Integer.MAX_VALUE`时，代表已映射文件中读取位置的字节偏移量达到了`Integer.MAX_VALUE`，也就是说已经读取了`Integer.MAX_VALUE`个字节的数据。

由于`MappedByteBuffer`是一个直接缓冲区，因此它可以处理的最大文件大小取决于操作系统和硬件等因素。在32位操作系统上，通常最大的文件大小为2GB，因为32位系统的内存地址空间最大只能表示2^32个地址，每个地址通常是1个字节。因此，当您尝试映射一个大于2GB的文件时，可能会抛出`java.lang.OutOfMemoryError`异常。

在64位操作系统上，`MappedByteBuffer`可以处理更大的文件，因为64位系统的内存地址空间可以表示更多的地址。具体而言，`MappedByteBuffer`最大可以处理的文件大小为`Long.MAX_VALUE`字节，也就是9,223,372,036,854,775,807字节，即约等于9.2EB（exabytes，即十亿GB）。但是，实际上可以处理的最大文件大小还取决于操作系统和硬件等因素。
``


# 2、数据查询——**也能分桶**

在得到k在某个分桶时，不急着直接排序，因为单个桶也是非常大的
所以，可以对这个桶，在进行分桶
这样，得到的 最小的桶在进行排序 就非常快了

核心代码时：
`//都减去桶开头的值 只有了后56位，再去右移45位，只留11位数字 用于分桶2048个中
bucket.get((int) ((value - rangeBegin) >> DIVISOR_POWER))
.add(new Data(value, dataFileNum, offset)
);`

解析完毕