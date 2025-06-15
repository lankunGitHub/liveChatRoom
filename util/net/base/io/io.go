package io

import (
	"liveChatroom/util/net/base/buffer"
	"syscall"
	"unsafe"
)

// Reader 高性能读取器
type Reader struct {
	fd int
}

// Writer 高性能写入器
type Writer struct {
	fd int
}

// NewReader 创建读取器
func NewReader(fd int) *Reader {
	return &Reader{fd: fd}
}

// NewWriter 创建写入器
func NewWriter(fd int) *Writer {
	return &Writer{fd: fd}
}

// ==================== 基础I/O操作 ====================

// Read 读取数据到缓冲区
func (r *Reader) Read(buf []byte) (int, error) {
	n, err := syscall.Read(r.fd, buf)
	if err != nil {
		return 0, err
	}
	return n, nil
}

// ReadToBuffer 读取数据到Buffer
func (r *Reader) ReadToBuffer(buf buffer.Buffer) (int, error) {
	// 获取可用空间
	available := buf.Available()
	if available == 0 {
		// 尝试扩展缓冲区
		if err := buf.Grow(4096); err != nil {
			return 0, err
		}
		available = buf.Available()
	}

	// 创建临时缓冲区读取数据
	tempBuf := make([]byte, available)
	n, err := r.Read(tempBuf)
	if err != nil {
		return 0, err
	}

	if n > 0 {
		// 写入到Buffer
		written, writeErr := buf.Write(tempBuf[:n])
		if writeErr != nil {
			return 0, writeErr
		}
		return written, nil
	}

	return 0, nil
}

// Write 写入数据
func (w *Writer) Write(buf []byte) (int, error) {
	n, err := syscall.Write(w.fd, buf)
	if err != nil {
		return 0, err
	}
	return n, nil
}

// WriteFromBuffer 从Buffer写入数据
func (w *Writer) WriteFromBuffer(buf buffer.Buffer) (int, error) {
	data := buf.ReadAll()
	if len(data) == 0 {
		return 0, nil
	}
	return w.Write(data)
}

// ==================== 零拷贝I/O操作 ====================

// Readv 向量读取（零拷贝）
func (r *Reader) Readv(iovecs []syscall.Iovec) (int, error) {
	n, _, errno := syscall.Syscall(syscall.SYS_READV,
		uintptr(r.fd),
		uintptr(unsafe.Pointer(&iovecs[0])),
		uintptr(len(iovecs)))
	if errno != 0 {
		return 0, errno
	}
	return int(n), nil
}

// Writev 向量写入（零拷贝）
func (w *Writer) Writev(iovecs []syscall.Iovec) (int, error) {
	n, _, errno := syscall.Syscall(syscall.SYS_WRITEV,
		uintptr(w.fd),
		uintptr(unsafe.Pointer(&iovecs[0])),
		uintptr(len(iovecs)))
	if errno != 0 {
		return 0, errno
	}
	return int(n), nil
}

// ReadToBufferv 向量读取到Buffer（零拷贝）
func (r *Reader) ReadToBufferv(buf buffer.Buffer) (int, error) {
	// 准备Buffer的iovec
	iovecs := buf.PrepareIovecs()
	if len(iovecs) == 0 {
		// Buffer没有可用空间，尝试扩展
		if err := buf.Grow(4096); err != nil {
			return 0, err
		}
		iovecs = buf.PrepareIovecs()
	}

	if len(iovecs) == 0 {
		return 0, nil
	}

	return buf.Readv(iovecs)
}

// WriteFromBufferv 从Buffer向量写入（零拷贝）
func (w *Writer) WriteFromBufferv(buf buffer.Buffer) (int, error) {
	iovecs := buf.PrepareIovecs()
	if len(iovecs) == 0 {
		return 0, nil
	}

	return w.Writev(iovecs)
}

// ==================== 高级I/O操作 ====================

// Sendfile 零拷贝文件传输
func Sendfile(outfd int, infd int, offset *int64, count int) (int, error) {
	n, _, errno := syscall.Syscall6(syscall.SYS_SENDFILE,
		uintptr(outfd),
		uintptr(infd),
		uintptr(unsafe.Pointer(offset)),
		uintptr(count),
		0, 0)
	if errno != 0 {
		return 0, errno
	}
	return int(n), nil
}

// Splice 零拷贝管道传输
func Splice(infd int, inoff *int64, outfd int, outoff *int64, count int, flags int) (int, error) {
	n, _, errno := syscall.Syscall6(syscall.SYS_SPLICE,
		uintptr(infd),
		uintptr(unsafe.Pointer(inoff)),
		uintptr(outfd),
		uintptr(unsafe.Pointer(outoff)),
		uintptr(count),
		uintptr(flags))
	if errno != 0 {
		return 0, errno
	}
	return int(n), nil
}

// ==================== 批量I/O操作 ====================

// ReadBatch 批量读取（减少系统调用）
func (r *Reader) ReadBatch(bufs [][]byte) (int, error) {
	if len(bufs) == 0 {
		return 0, nil
	}

	// 构建iovec数组
	iovecs := make([]syscall.Iovec, len(bufs))
	for i, buf := range bufs {
		if len(buf) > 0 {
			iovecs[i] = syscall.Iovec{
				Base: &buf[0],
				Len:  uint64(len(buf)),
			}
		}
	}

	return r.Readv(iovecs)
}

// WriteBatch 批量写入（减少系统调用）
func (w *Writer) WriteBatch(bufs [][]byte) (int, error) {
	if len(bufs) == 0 {
		return 0, nil
	}

	// 构建iovec数组
	iovecs := make([]syscall.Iovec, len(bufs))
	for i, buf := range bufs {
		if len(buf) > 0 {
			iovecs[i] = syscall.Iovec{
				Base: &buf[0],
				Len:  uint64(len(buf)),
			}
		}
	}

	return w.Writev(iovecs)
}

// ==================== 流式I/O操作 ====================

// ReadStream 流式读取器
type ReadStream struct {
	reader *Reader
	buffer buffer.Buffer
}

// WriteStream 流式写入器
type WriteStream struct {
	writer *Writer
	buffer buffer.Buffer
}

// NewReadStream 创建读取流
func NewReadStream(fd int, bufSize int) *ReadStream {
	return &ReadStream{
		reader: NewReader(fd),
		buffer: buffer.Get(bufSize),
	}
}

// NewWriteStream 创建写入流
func NewWriteStream(fd int, bufSize int) *WriteStream {
	return &WriteStream{
		writer: NewWriter(fd),
		buffer: buffer.Get(bufSize),
	}
}

// Read 从流读取数据
func (rs *ReadStream) Read(buf []byte) (int, error) {
	// 如果缓冲区没有足够数据，先读取
	if rs.buffer.Size() < len(buf) {
		_, err := rs.reader.ReadToBuffer(rs.buffer)
		if err != nil {
			return 0, err
		}
	}

	return rs.buffer.Read(buf)
}

// ReadFrame 读取指定长度的帧
func (rs *ReadStream) ReadFrame(frameLen int) ([]byte, bool) {
	// 确保有足够数据
	for rs.buffer.Size() < frameLen {
		_, err := rs.reader.ReadToBuffer(rs.buffer)
		if err != nil {
			return nil, false
		}
	}

	return rs.buffer.ReadFrame(frameLen)
}

// Write 写入数据到流
func (ws *WriteStream) Write(buf []byte) (int, error) {
	return ws.buffer.Write(buf)
}

// Flush 刷新写入缓冲区
func (ws *WriteStream) Flush() error {
	_, err := ws.writer.WriteFromBuffer(ws.buffer)
	return err
}

// Close 关闭流并归还缓冲区
func (rs *ReadStream) Close() {
	buffer.Put(rs.buffer)
}

// Close 关闭流并归还缓冲区
func (ws *WriteStream) Close() {
	ws.Flush()
	buffer.Put(ws.buffer)
}

// ==================== 工具函数 ====================

// CopyZero 零拷贝数据传输
func CopyZero(dst, src int, size int) (int, error) {
	// 优先使用sendfile（如果src是文件）
	n, err := Sendfile(dst, src, nil, size)
	if err == nil {
		return n, nil
	}

	// fallback到splice（如果支持）
	n, err = Splice(src, nil, dst, nil, size, 0)
	if err == nil {
		return n, nil
	}

	// 最后fallback到普通copy
	return copyRegular(dst, src, size)
}

// copyRegular 常规拷贝
func copyRegular(dst, src int, size int) (int, error) {
	reader := NewReader(src)
	writer := NewWriter(dst)

	buf := make([]byte, 32*1024) // 32KB缓冲区
	total := 0

	for total < size {
		toRead := len(buf)
		if remaining := size - total; remaining < toRead {
			toRead = remaining
		}

		n, err := reader.Read(buf[:toRead])
		if err != nil {
			return total, err
		}
		if n == 0 {
			break
		}

		written, err := writer.Write(buf[:n])
		if err != nil {
			return total, err
		}

		total += written
		if written != n {
			break
		}
	}

	return total, nil
}
