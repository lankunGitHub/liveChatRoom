package buffer

import (
	"io"
	"syscall"
)

// CopyBuffer 使用缓冲区进行高效拷贝
func CopyBuffer(dst io.Writer, src io.Reader, buf Buffer) (int64, error) {
	var written int64

	for {
		// 重置缓冲区
		buf.Reset()

		// 读取数据到缓冲区
		tempBuf := make([]byte, buf.Cap())
		nr, er := src.Read(tempBuf)
		if nr > 0 {
			// 写入缓冲区
			buf.Write(tempBuf[:nr])

			// 从缓冲区读取并写入目标
			data := buf.ReadAll()
			nw, ew := dst.Write(data)
			if nw > 0 {
				written += int64(nw)
			}
			if ew != nil {
				return written, ew
			}
			if nr != nw {
				return written, io.ErrShortWrite
			}
		}
		if er != nil {
			if er != io.EOF {
				return written, er
			}
			break
		}
	}
	return written, nil
}

// ReadFrame 从Reader中读取指定长度的帧数据
func ReadFrame(reader io.Reader, frameLen int, buf Buffer) ([]byte, error) {
	if frameLen <= 0 {
		return nil, nil
	}

	// 确保缓冲区有足够空间
	for buf.Size() < frameLen {
		tempBuf := make([]byte, 4096)
		n, err := reader.Read(tempBuf)
		if err != nil {
			return nil, err
		}
		if n > 0 {
			buf.Write(tempBuf[:n])
		}
	}

	frame, ok := buf.ReadFrame(frameLen)
	if !ok {
		return nil, io.EOF
	}
	return frame, nil
}

// WriteFrames 批量写入帧数据
func WriteFrames(writer io.Writer, frames [][]byte, buf Buffer) error {
	buf.Reset()

	// 将所有帧写入缓冲区
	for _, frame := range frames {
		if _, err := buf.Write(frame); err != nil {
			return err
		}
	}

	// 一次性写入目标
	data := buf.ReadAll()
	_, err := writer.Write(data)
	return err
}

// PrepareWriteVector 准备写入向量，用于零拷贝操作
func PrepareWriteVector(data [][]byte) []syscall.Iovec {
	iovecs := make([]syscall.Iovec, 0, len(data))

	for _, chunk := range data {
		if len(chunk) > 0 {
			iovecs = append(iovecs, syscall.Iovec{
				Base: &chunk[0],
				Len:  uint64(len(chunk)),
			})
		}
	}

	return iovecs
}

// BufferedWriter 缓冲写入器
type BufferedWriter struct {
	writer io.Writer
	buf    Buffer
}

// NewBufferedWriter 创建缓冲写入器
func NewBufferedWriter(writer io.Writer, bufSize int) *BufferedWriter {
	return &BufferedWriter{
		writer: writer,
		buf:    Get(bufSize),
	}
}

// Write 写入数据
func (bw *BufferedWriter) Write(data []byte) (int, error) {
	return bw.buf.Write(data)
}

// Flush 刷新缓冲区
func (bw *BufferedWriter) Flush() error {
	data := bw.buf.ReadAll()
	if len(data) == 0 {
		return nil
	}

	_, err := bw.writer.Write(data)
	return err
}

// Close 关闭并归还缓冲区
func (bw *BufferedWriter) Close() error {
	if err := bw.Flush(); err != nil {
		return err
	}

	Put(bw.buf)
	bw.buf = nil
	return nil
}

// BufferedReader 缓冲读取器
type BufferedReader struct {
	reader io.Reader
	buf    Buffer
}

// NewBufferedReader 创建缓冲读取器
func NewBufferedReader(reader io.Reader, bufSize int) *BufferedReader {
	return &BufferedReader{
		reader: reader,
		buf:    Get(bufSize),
	}
}

// Read 读取数据
func (br *BufferedReader) Read(data []byte) (int, error) {
	// 如果缓冲区没有足够数据，先从reader读取
	if br.buf.Size() < len(data) {
		tempBuf := make([]byte, br.buf.Cap())
		n, err := br.reader.Read(tempBuf)
		if err != nil && err != io.EOF {
			return 0, err
		}
		if n > 0 {
			br.buf.Write(tempBuf[:n])
		}
	}

	return br.buf.Read(data)
}

// Peek 预览数据
func (br *BufferedReader) Peek(n int) ([]byte, error) {
	// 确保缓冲区有足够数据
	for br.buf.Size() < n {
		tempBuf := make([]byte, 4096)
		nr, err := br.reader.Read(tempBuf)
		if err != nil {
			return br.buf.Peek(br.buf.Size())
		}
		if nr > 0 {
			br.buf.Write(tempBuf[:nr])
		}
	}

	return br.buf.Peek(n)
}

// Close 关闭并归还缓冲区
func (br *BufferedReader) Close() error {
	Put(br.buf)
	br.buf = nil
	return nil
}
