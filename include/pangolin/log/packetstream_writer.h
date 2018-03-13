/* This file is part of the Pangolin Project.
 * http://github.com/stevenlovegrove/Pangolin
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#pragma once

#include <ostream>
#include <queue>
#include <atomic>

#include <pangolin/log/packetstream.h>
#include <pangolin/log/packetstream_source.h>
#include <pangolin/utils/file_utils.h>
#include <pangolin/utils/threadedfilebuf.h>

namespace pangolin
{

// PacketStreamWriter represents a thread-safe class for writing multiplexed
// packets from different sources to disk.
class PANGOLIN_EXPORT PacketStreamWriter
{
public:
    PacketStreamWriter();

    PacketStreamWriter(const std::string& filename);

    ~PacketStreamWriter();

    void Open(const std::string& filename);

    void Close();

    // Does not write footer or index.
    void ForceClose();

    // Writes to the stream immediately upon add. Return source id # and writes
    // source id # to argument struct
    PacketStreamSourceId AddSource(PacketStreamSource& source);

    // If constructor is called inline
    PacketStreamSourceId AddSource(const PacketStreamSource& source);

    void WriteSourcePacket(
        PacketStreamSourceId src, const char* source,const int64_t receive_time_us,
        size_t sourcelen, const picojson::value& meta = picojson::value()
    );


    const std::vector<PacketStreamSource>& Sources() const {
        return _sources;
    }

    bool IsOpen() const {
        return _stream.is_open();
    }

private:
    using Buffer = std::vector<char>;

    static void BufferHeader(Buffer& writer);
    static void BufferCompressedUnsignedInt(Buffer& writer, size_t n);
    static void BufferTag(Buffer& writer, const pangoTagType tag);
    static void BufferSourceDescription(Buffer& writer, const PacketStreamSource& source);

    template<typename T>
    static void BufferArg(Buffer& writer, const T& v);

    // Returns a free buffer from the buffer pool
    Buffer GetWriteBuffer();
    // Submit a buffer to be written to the stream
    void SubmitWriteBuffer(Buffer&& b);
    // Return a buffer to the buffer pool for future use
    void PutWriteBuffer(Buffer&& b);

    // For stream read/write synchronization. Note that this is NOT the same as
    // time synchronization on playback of iPacketStreams.
    void WriteSyncToStream();

    // Writes the end of the stream data, including the index. Does NOT close
    // the underlying ostream.
    void WriteEndToStream();

    void WriteLoop();

    std::ofstream _stream;
    bool _indexable;

    std::vector<PacketStreamSource> _sources;
    size_t _bytes_written;
    std::recursive_mutex _lock;

    // Single thread handling io
    std::thread write_thread;
    std::atomic<bool> should_run;

    // queue of data to write
    std::deque<Buffer> to_write;

    // queue of spare pre-allocated buffers
    std::vector<Buffer> alloc_pool;

    std::mutex queue_write_mutex;
    std::mutex queue_pool_mutex;
    std::condition_variable queue_add_cond;
};

}
