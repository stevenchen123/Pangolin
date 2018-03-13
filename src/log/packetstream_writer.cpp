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

#include <pangolin/log/packetstream_writer.h>
#include <pangolin/utils/file_utils.h>
#include <pangolin/utils/timer.h>

using std::ios;
using std::lock_guard;

namespace pangolin
{

inline void PacketStreamWriter::BufferCompressedUnsignedInt(PacketStreamWriter::Buffer& writer, size_t n)
{
    while (n >= 0x80)
    {
        writer.push_back(0x80 | (n & 0x7F));
        n >>= 7;
    }
    writer.push_back(static_cast<unsigned char>(n));
}


inline void PacketStreamWriter::BufferTag(PacketStreamWriter::Buffer& writer, const pangoTagType tag)
{
    using pc = unsigned char*;
    writer.insert(writer.end(), pc(&tag), pc(&tag) + TAG_LENGTH);
}

inline void PacketStreamWriter::BufferSourceDescription(PacketStreamWriter::Buffer& writer, const PacketStreamSource& source)
{
    picojson::value serialize;
    serialize[pss_src_driver] = source.driver;
    serialize[pss_src_id] = source.id;
    serialize[pss_src_uri] = source.uri;
    serialize[pss_src_info] = source.info;
    serialize[pss_src_version] = source.version;
    serialize[pss_src_packet][pss_pkt_alignment_bytes] = source.data_alignment_bytes;
    serialize[pss_src_packet][pss_pkt_definitions] = source.data_definitions;
    serialize[pss_src_packet][pss_pkt_size_bytes] = source.data_size_bytes;

    BufferTag(writer, TAG_ADD_SOURCE);
    serialize.serialize(std::back_inserter(writer), true);
}

template<typename T>
inline void PacketStreamWriter::BufferArg(PacketStreamWriter::Buffer& writer, const T& v)
{
    using pc = unsigned char*;
    writer.insert(writer.end(), pc(&v), pc(&v) + sizeof(T));
}

inline picojson::value SourceStats(const std::vector<PacketStreamSource>& srcs)
{
    picojson::value stat;
    stat["num_sources"] = srcs.size();
    stat["src_packet_index"] = picojson::array();
    stat["src_packet_times"] = picojson::array();

    for(auto& src : srcs) {
        picojson::array pkt_index, pkt_times;
        for (const PacketStreamSource::PacketInfo& frame : src.index) {
            pkt_index.emplace_back(frame.pos);
            pkt_times.emplace_back(frame.capture_time);
        }
        stat["src_packet_index"].push_back(std::move(pkt_index));
        stat["src_packet_times"].push_back(std::move(pkt_times));
    }
    return stat;
}

PacketStreamWriter::PacketStreamWriter()
    : _indexable(false), _bytes_written(0)
{
    _stream.exceptions(std::ostream::badbit);
}

PacketStreamWriter::PacketStreamWriter(const std::string& filename)
    : _indexable(false), _bytes_written(0)
{
    _stream.exceptions(std::ostream::badbit);
    Open(filename);
}

PacketStreamWriter::~PacketStreamWriter() {
    Close();
}

void PacketStreamWriter::Open(const std::string& filename)
{
    Close();
    // Set the read / write buffer to 0, since we'll be buffering ourselves
    _stream.rdbuf()->pubsetbuf(0, 0);
    _stream.open(filename, std::ios_base::binary);

    if(_stream.is_open()) {
        _indexable = !IsPipe(filename);
        _bytes_written = 0;
        write_thread = std::thread(&PacketStreamWriter::WriteLoop, this);
    }
}

void PacketStreamWriter::Close()
{
    // Close any writing
    if(write_thread.joinable()) {
        should_run = false;
        write_thread.join();
    }

    // Close stream
    if(_stream.is_open()) {
        _stream.close();
    }
}

void PacketStreamWriter::ForceClose()
{
    // TODO: Kill write thread
    Close();
}

static inline const std::string CurrentTimeStr()
{
    time_t time_now = time(0);
    struct tm time_struct = *localtime(&time_now);
    char buffer[80];
    strftime(buffer, sizeof(buffer), "%Y-%m-%d %X", &time_struct);
    return buffer;
}

void PacketStreamWriter::BufferHeader(Buffer& writer)
{
    picojson::value pango;
    pango["pangolin_version"] = PANGOLIN_VERSION_STRING;
    pango["time_us"] = Time_us(TimeNow());
    pango["date_created"] = CurrentTimeStr();
    pango["endian"] = "little_endian";

    writer.insert(writer.end(), PANGO_MAGIC.c_str(), PANGO_MAGIC.c_str() + PANGO_MAGIC.size());
    BufferTag(writer, TAG_PANGO_HDR);
    pango.serialize(std::back_inserter(writer), true);
}

PacketStreamWriter::Buffer PacketStreamWriter::GetWriteBuffer()
{
    std::lock_guard<std::mutex> l(queue_pool_mutex);
    if(alloc_pool.empty()) {
        return Buffer();
    }else{
        Buffer b = std::move(alloc_pool.back());
        alloc_pool.pop_back();
        return b;
    }
}

void PacketStreamWriter::PutWriteBuffer(Buffer&& b)
{
    std::lock_guard<std::mutex> l(queue_pool_mutex);
    // Reset the size, but not the capacity, before returning.
    b.clear();
    alloc_pool.push_back(std::move(b));
}

void PacketStreamWriter::SubmitWriteBuffer(Buffer&& b)
{
    std::unique_lock<std::mutex> l(queue_write_mutex);
    to_write.push_back(std::move(b));
    queue_add_cond.notify_one();
}

PacketStreamSourceId PacketStreamWriter::AddSource(PacketStreamSource& source)
{
    source.id = AddSource(const_cast<const PacketStreamSource&>(source));
    return source.id;
}

PacketStreamSourceId PacketStreamWriter::AddSource(const PacketStreamSource& source)
{
    PacketStreamSourceId r = _sources.size();
    _sources.push_back(source);
    _sources.back().id = r;

    Buffer buf = GetWriteBuffer();
    BufferSourceDescription(buf, _sources.back());
    SubmitWriteBuffer(std::move(buf));

    return r;
}

void PacketStreamWriter::WriteSourcePacket(PacketStreamSourceId src, const char* source, const int64_t receive_time_us, size_t sourcelen, const picojson::value& meta)
{
    sdfsdfd // not thread safe...
    _sources[src].index.push_back({_stream.tellp(), receive_time_us});

    Buffer buf = GetWriteBuffer();

    if (!meta.is<picojson::null>()) {
        BufferTag(buf, TAG_SRC_JSON);
        BufferCompressedUnsignedInt(buf, src);
        meta.serialize(std::back_inserter(buf), false);
    }

    BufferTag(buf, TAG_SRC_PACKET);
    BufferArg(buf, receive_time_us);
    BufferCompressedUnsignedInt(buf, src);

    if (_sources[src].data_size_bytes) {
        if (sourcelen != static_cast<size_t>(_sources[src].data_size_bytes))
            throw std::runtime_error("oPacketStream::writePacket --> Tried to write a fixed-size packet with bad size.");
    } else {
        BufferCompressedUnsignedInt(buf, sourcelen);
    }

    buf.insert(buf.end(), source, source + sourcelen);
    _bytes_written += buf.size();
    SubmitWriteBuffer(std::move(buf));
}

void PacketStreamWriter::WriteSyncToStream()
{
    Buffer b = GetWriteBuffer();
    for (unsigned i = 0; i < 10; ++i)
        BufferTag(b, TAG_PANGO_SYNC);
    SubmitWriteBuffer(std::move(b));
}

void PacketStreamWriter::WriteEndToStream()
{
    if (!_indexable)
        return;

    const uint64_t indexpos = static_cast<uint64_t>(_stream.tellp());
    Buffer b = GetWriteBuffer();
    BufferTag(b, TAG_PANGO_STATS);
    SourceStats(_sources).serialize(std::back_inserter(b), false);
    BufferTag(b, TAG_PANGO_FOOTER);
    BufferArg(b,indexpos);
    _stream.write(b.data(), b.size());
}

void PacketStreamWriter::WriteLoop()
{
    Buffer buffer = GetWriteBuffer();
    BufferHeader(buffer);
    SubmitWriteBuffer(std::move(buffer));

    // Write out anything in the to_write queue
    while(should_run && _stream.good()) {
        {
            std::lock_guard<std::mutex> lock(queue_write_mutex);
            while(to_write.empty() && should_run) {
                queue_add_cond.wait(lock);
            }
            if(!(should_run && _stream.good()) ) break;
            buffer = std::move(to_write.front());
            to_write.pop_front();
        }

        _stream.write(buffer.data(), buffer.size());

        // Give back our buffer
        {
            std::lock_guard<std::mutex> lock(queue_pool_mutex);
            alloc_pool.push_back(std::move(buffer));
        }
    }

    if (_stream.good() && _indexable) {
        WriteEndToStream();
    }
}

}
