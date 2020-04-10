/**
 * @file unit-ChunkedBuffer.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2019 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * Tests the `ChunkedBuffer` class.
 */

#include "tiledb/sm/tile/chunked_buffer.h"

#include <catch.hpp>
#include <iostream>

using namespace tiledb::sm;

TEST_CASE(
    "ChunkedBuffer: Test discrete, fixed size IO 1",
    "[ChunkedBuffer][discrete_fixed_io]") {
  std::cerr << "JOE 1" << std::endl;
}

TEST_CASE(
    "ChunkedBuffer: Test discrete, fixed size IO 2",
    "[ChunkedBuffer][discrete_fixed_io]") {
  std::cerr << "JOE 1" << std::endl;

  // Instantiate a test ChunkedBuffer.
  ChunkedBuffer chunked_buffer;

  std::cerr << "JOE 2" << std::endl;
}

TEST_CASE(
    "ChunkedBuffer: Test discrete, fixed size IO 3",
    "[ChunkedBuffer][discrete_fixed_io]") {
  std::cerr << "JOE 1" << std::endl;

  // Instantiate a test ChunkedBuffer.
  ChunkedBuffer chunked_buffer;

  std::cerr << "JOE 2" << std::endl;

  // Create a buffer to write to the test ChunkedBuffer.
  const int64_t buffer_size = 1024 * 1024 * 3;
  uint64_t* const write_buffer = static_cast<uint64_t*>(malloc(buffer_size));
  const uint64_t buffer_len = buffer_size / sizeof(uint64_t);
  for (uint64_t i = 0; i < buffer_len; ++i) {
    write_buffer[i] = i;
  }

  std::cerr << "JOE 3" << std::endl;
}

TEST_CASE(
    "ChunkedBuffer: Test discrete, fixed size IO 4",
    "[ChunkedBuffer][discrete_fixed_io]") {
  std::cerr << "JOE 1" << std::endl;

  // Instantiate a test ChunkedBuffer.
  ChunkedBuffer chunked_buffer;

  std::cerr << "JOE 2" << std::endl;

  // Create a buffer to write to the test ChunkedBuffer.
  const int64_t buffer_size = 1024 * 1024 * 3;
  uint64_t* const write_buffer = static_cast<uint64_t*>(malloc(buffer_size));
  REQUIRE(write_buffer);
  const uint64_t buffer_len = buffer_size / sizeof(uint64_t);
  for (uint64_t i = 0; i < buffer_len; ++i) {
    write_buffer[i] = i;
  }

  std::cerr << "JOE 3" << std::endl;

  // Attempt a write before initializing the test ChunkedBuffer.
  uint64_t write_offset = 0;
  REQUIRE(!chunked_buffer.write(write_buffer, buffer_size, write_offset).ok());

  std::cerr << "JOE 4 * " << chunked_buffer.size() << std::endl;
  std::cerr << "JOE 4 ** " << chunked_buffer.capacity() << std::endl;
  std::cerr << "JOE 4 *** " << chunked_buffer.nchunks() << std::endl;
  std::cerr << "JOE 4 **** " << buffer_len << std::endl;

  uint64_t read_buffer[buffer_len];
  std::cerr << "JOE 4 ***** " << sizeof(read_buffer) << std::endl;
}

TEST_CASE(
    "ChunkedBuffer: Test discrete, fixed size IO 4.1",
    "[ChunkedBuffer][discrete_fixed_io]") {
  std::cerr << "JOE 1.1" << std::endl;

  // Instantiate a test ChunkedBuffer.
  ChunkedBuffer chunked_buffer;

  std::cerr << "JOE 2" << std::endl;

  // Create a buffer to write to the test ChunkedBuffer.
  const uint64_t buffer_size = 1024 * 1024 * 3;
  const uint64_t buffer_len = buffer_size / sizeof(uint64_t);

  std::cerr << "JOE 3" << std::endl;

  std::cerr << "JOE 4 * " << chunked_buffer.size() << std::endl;
  std::cerr << "JOE 4 ** " << chunked_buffer.capacity() << std::endl;
  std::cerr << "JOE 4 *** " << chunked_buffer.nchunks() << std::endl;
  std::cerr << "JOE 4 **** " << buffer_len << std::endl;

  uint64_t read_offset = 0;
  uint64_t read_buffer[buffer_len];
  REQUIRE(!chunked_buffer.read(&read_buffer[0], buffer_size, read_offset).ok());
}

TEST_CASE(
    "ChunkedBuffer: Test discrete, fixed size IO 5",
    "[ChunkedBuffer][discrete_fixed_io]") {
  std::cerr << "JOE 1" << std::endl;

  // Instantiate a test ChunkedBuffer.
  ChunkedBuffer chunked_buffer;

  std::cerr << "JOE 2" << std::endl;

  // Create a buffer to write to the test ChunkedBuffer.
  const uint64_t buffer_size = 1024 * 1024 * 3;
  uint64_t* const write_buffer = static_cast<uint64_t*>(malloc(buffer_size));
  const uint64_t buffer_len = buffer_size / sizeof(uint64_t);
  for (uint64_t i = 0; i < buffer_len; ++i) {
    write_buffer[i] = i;
  }

  std::cerr << "JOE 3" << std::endl;

  // Attempt a write before initializing the test ChunkedBuffer.
  uint64_t write_offset = 0;
  REQUIRE(!chunked_buffer.write(write_buffer, buffer_size, write_offset).ok());

  std::cerr << "JOE 4" << chunked_buffer.size() << std::endl;

  // TODO crashes here

  // Attempt a read before initializing the test ChunkedBuffer.
  uint64_t read_offset = 0;
  uint64_t read_buffer[buffer_len];
  REQUIRE(!chunked_buffer.read(read_buffer, buffer_size, read_offset).ok());

  std::cerr << "JOE 5" << std::endl;
}

TEST_CASE(
    "ChunkedBuffer: Test discrete, fixed size IO 6",
    "[ChunkedBuffer][discrete_fixed_io]") {
  std::cerr << "JOE 1" << std::endl;

  // Instantiate a test ChunkedBuffer.
  ChunkedBuffer chunked_buffer;

  std::cerr << "JOE 2" << std::endl;

  // Create a buffer to write to the test ChunkedBuffer.
  const int64_t buffer_size = 1024 * 1024 * 3;
  uint64_t* const write_buffer = static_cast<uint64_t*>(malloc(buffer_size));
  const uint64_t buffer_len = buffer_size / sizeof(uint64_t);
  for (uint64_t i = 0; i < buffer_len; ++i) {
    write_buffer[i] = i;
  }

  std::cerr << "JOE 3" << std::endl;

  // Attempt a write before initializing the test ChunkedBuffer.
  uint64_t write_offset = 0;
  REQUIRE(!chunked_buffer.write(write_buffer, buffer_size, write_offset).ok());

  std::cerr << "JOE 4" << std::endl;

  // Attempt a read before initializing the test ChunkedBuffer.
  uint64_t read_offset = 0;
  uint64_t read_buffer[buffer_len];
  REQUIRE(!chunked_buffer.read(read_buffer, buffer_size, read_offset).ok());

  std::cerr << "JOE 5" << std::endl;

  // Attempt an alloc before initializing the test ChunkedBuffer.
  size_t chunk_idx = 0;
  void* chunk_buffer = nullptr;
  REQUIRE(!chunked_buffer.alloc_discrete(chunk_idx, &chunk_buffer).ok());
  REQUIRE(!chunk_buffer);

  std::cerr << "JOE 6" << std::endl;
}

TEST_CASE(
    "ChunkedBuffer: Test discrete, fixed size IO 7",
    "[ChunkedBuffer][discrete_fixed_io]") {
  std::cerr << "JOE 1" << std::endl;

  // Instantiate a test ChunkedBuffer.
  ChunkedBuffer chunked_buffer;

  std::cerr << "JOE 2" << std::endl;

  // Create a buffer to write to the test ChunkedBuffer.
  const int64_t buffer_size = 1024 * 1024 * 3;
  uint64_t* const write_buffer = static_cast<uint64_t*>(malloc(buffer_size));
  const uint64_t buffer_len = buffer_size / sizeof(uint64_t);
  for (uint64_t i = 0; i < buffer_len; ++i) {
    write_buffer[i] = i;
  }

  std::cerr << "JOE 3" << std::endl;

  // Attempt a write before initializing the test ChunkedBuffer.
  uint64_t write_offset = 0;
  REQUIRE(!chunked_buffer.write(write_buffer, buffer_size, write_offset).ok());

  std::cerr << "JOE 4" << std::endl;

  // Attempt a read before initializing the test ChunkedBuffer.
  uint64_t read_offset = 0;
  uint64_t read_buffer[buffer_len];
  REQUIRE(!chunked_buffer.read(read_buffer, buffer_size, read_offset).ok());

  std::cerr << "JOE 5" << std::endl;

  // Attempt an alloc before initializing the test ChunkedBuffer.
  size_t chunk_idx = 0;
  void* chunk_buffer = nullptr;
  REQUIRE(!chunked_buffer.alloc_discrete(chunk_idx, &chunk_buffer).ok());
  REQUIRE(!chunk_buffer);

  std::cerr << "JOE 6" << std::endl;

  // Attempt a set before initializing the test ChunkedBuffer.
  chunk_buffer = nullptr;
  REQUIRE(!chunked_buffer.set_contigious(chunk_buffer).ok());
  REQUIRE(!chunk_buffer);

  std::cerr << "JOE 7" << std::endl;
}

TEST_CASE(
    "ChunkedBuffer: Test discrete, fixed size IO 8",
    "[ChunkedBuffer][discrete_fixed_io]") {
  std::cerr << "JOE 1" << std::endl;

  // Instantiate a test ChunkedBuffer.
  ChunkedBuffer chunked_buffer;

  std::cerr << "JOE 2" << std::endl;

  // Create a buffer to write to the test ChunkedBuffer.
  const int64_t buffer_size = 1024 * 1024 * 3;
  uint64_t* const write_buffer = static_cast<uint64_t*>(malloc(buffer_size));
  const uint64_t buffer_len = buffer_size / sizeof(uint64_t);
  for (uint64_t i = 0; i < buffer_len; ++i) {
    write_buffer[i] = i;
  }

  std::cerr << "JOE 3" << std::endl;

  // Attempt a write before initializing the test ChunkedBuffer.
  uint64_t write_offset = 0;
  REQUIRE(!chunked_buffer.write(write_buffer, buffer_size, write_offset).ok());

  std::cerr << "JOE 4" << std::endl;

  // Attempt a read before initializing the test ChunkedBuffer.
  uint64_t read_offset = 0;
  uint64_t read_buffer[buffer_len];
  REQUIRE(!chunked_buffer.read(read_buffer, buffer_size, read_offset).ok());

  std::cerr << "JOE 5" << std::endl;

  // Attempt an alloc before initializing the test ChunkedBuffer.
  size_t chunk_idx = 0;
  void* chunk_buffer = nullptr;
  REQUIRE(!chunked_buffer.alloc_discrete(chunk_idx, &chunk_buffer).ok());
  REQUIRE(!chunk_buffer);

  std::cerr << "JOE 6" << std::endl;

  // Attempt a set before initializing the test ChunkedBuffer.
  chunk_buffer = nullptr;
  REQUIRE(!chunked_buffer.set_contigious(chunk_buffer).ok());
  REQUIRE(!chunk_buffer);

  std::cerr << "JOE 7" << std::endl;

  // Initialize the ChunkedBuffer.
  REQUIRE(buffer_size % sizeof(uint64_t) == 0);
  const size_t chunk_size = 1024 * 100;
  REQUIRE(buffer_size % chunk_size != 0);
  const size_t nchunks = (buffer_size / chunk_size) + 1;
  const size_t last_chunk_size = buffer_size % chunk_size;
  REQUIRE(chunk_size != last_chunk_size);
  chunked_buffer.init_fixed_size(
      ChunkedBuffer::BufferAddressing::DISCRETE, buffer_size, chunk_size);
  REQUIRE(chunked_buffer.capacity() == buffer_size);
  REQUIRE(chunked_buffer.size() == 0);
  REQUIRE(chunked_buffer.nchunks() == nchunks);

  std::cerr << "JOE 8" << std::endl;
}

TEST_CASE(
    "ChunkedBuffer: Test discrete, fixed size IO 10",
    "[ChunkedBuffer][discrete_fixed_io]") {
  std::cerr << "JOE 1" << std::endl;

  // Instantiate a test ChunkedBuffer.
  ChunkedBuffer chunked_buffer;

  std::cerr << "JOE 2" << std::endl;

  // Create a buffer to write to the test ChunkedBuffer.
  const int64_t buffer_size = 1024 * 1024 * 3;
  uint64_t* const write_buffer = static_cast<uint64_t*>(malloc(buffer_size));
  const uint64_t buffer_len = buffer_size / sizeof(uint64_t);
  for (uint64_t i = 0; i < buffer_len; ++i) {
    write_buffer[i] = i;
  }

  std::cerr << "JOE 3" << std::endl;

  // Attempt a write before initializing the test ChunkedBuffer.
  uint64_t write_offset = 0;
  REQUIRE(!chunked_buffer.write(write_buffer, buffer_size, write_offset).ok());

  std::cerr << "JOE 4" << std::endl;

  // Attempt a read before initializing the test ChunkedBuffer.
  uint64_t read_offset = 0;
  uint64_t read_buffer[buffer_len];
  REQUIRE(!chunked_buffer.read(read_buffer, buffer_size, read_offset).ok());

  std::cerr << "JOE 5" << std::endl;

  // Attempt an alloc before initializing the test ChunkedBuffer.
  size_t chunk_idx = 0;
  void* chunk_buffer = nullptr;
  REQUIRE(!chunked_buffer.alloc_discrete(chunk_idx, &chunk_buffer).ok());
  REQUIRE(!chunk_buffer);

  std::cerr << "JOE 6" << std::endl;

  // Attempt a set before initializing the test ChunkedBuffer.
  chunk_buffer = nullptr;
  REQUIRE(!chunked_buffer.set_contigious(chunk_buffer).ok());
  REQUIRE(!chunk_buffer);

  std::cerr << "JOE 7" << std::endl;

  // Initialize the ChunkedBuffer.
  REQUIRE(buffer_size % sizeof(uint64_t) == 0);
  const size_t chunk_size = 1024 * 100;
  REQUIRE(buffer_size % chunk_size != 0);
  const size_t nchunks = (buffer_size / chunk_size) + 1;
  const size_t last_chunk_size = buffer_size % chunk_size;
  REQUIRE(chunk_size != last_chunk_size);
  chunked_buffer.init_fixed_size(
      ChunkedBuffer::BufferAddressing::DISCRETE, buffer_size, chunk_size);
  REQUIRE(chunked_buffer.capacity() == buffer_size);
  REQUIRE(chunked_buffer.size() == 0);
  REQUIRE(chunked_buffer.nchunks() == nchunks);

  std::cerr << "JOE 8" << std::endl;

  // Verify all chunks are unallocated.
  for (size_t i = 0; i < chunked_buffer.nchunks(); ++i) {
    std::cerr << "JOE 9" << std::endl;
    void* chunk_buffer;
    REQUIRE(chunked_buffer.internal_buffer(i, &chunk_buffer).ok());
    REQUIRE(!chunk_buffer);
  }

  std::cerr << "JOE 10" << std::endl;
}

TEST_CASE(
    "ChunkedBuffer: Test discrete, fixed size IO 11",
    "[ChunkedBuffer][discrete_fixed_io]") {
  std::cerr << "JOE 1" << std::endl;

  // Instantiate a test ChunkedBuffer.
  ChunkedBuffer chunked_buffer;

  std::cerr << "JOE 2" << std::endl;

  // Create a buffer to write to the test ChunkedBuffer.
  const int64_t buffer_size = 1024 * 1024 * 3;
  uint64_t* const write_buffer = static_cast<uint64_t*>(malloc(buffer_size));
  const uint64_t buffer_len = buffer_size / sizeof(uint64_t);
  for (uint64_t i = 0; i < buffer_len; ++i) {
    write_buffer[i] = i;
  }

  std::cerr << "JOE 3" << std::endl;

  // Attempt a write before initializing the test ChunkedBuffer.
  uint64_t write_offset = 0;
  REQUIRE(!chunked_buffer.write(write_buffer, buffer_size, write_offset).ok());

  std::cerr << "JOE 4" << std::endl;

  // Attempt a read before initializing the test ChunkedBuffer.
  uint64_t read_offset = 0;
  uint64_t read_buffer[buffer_len];
  REQUIRE(!chunked_buffer.read(read_buffer, buffer_size, read_offset).ok());

  std::cerr << "JOE 5" << std::endl;

  // Attempt an alloc before initializing the test ChunkedBuffer.
  size_t chunk_idx = 0;
  void* chunk_buffer = nullptr;
  REQUIRE(!chunked_buffer.alloc_discrete(chunk_idx, &chunk_buffer).ok());
  REQUIRE(!chunk_buffer);

  std::cerr << "JOE 6" << std::endl;

  // Attempt a set before initializing the test ChunkedBuffer.
  chunk_buffer = nullptr;
  REQUIRE(!chunked_buffer.set_contigious(chunk_buffer).ok());
  REQUIRE(!chunk_buffer);

  std::cerr << "JOE 7" << std::endl;

  // Initialize the ChunkedBuffer.
  REQUIRE(buffer_size % sizeof(uint64_t) == 0);
  const size_t chunk_size = 1024 * 100;
  REQUIRE(buffer_size % chunk_size != 0);
  const size_t nchunks = (buffer_size / chunk_size) + 1;
  const size_t last_chunk_size = buffer_size % chunk_size;
  REQUIRE(chunk_size != last_chunk_size);
  chunked_buffer.init_fixed_size(
      ChunkedBuffer::BufferAddressing::DISCRETE, buffer_size, chunk_size);
  REQUIRE(chunked_buffer.capacity() == buffer_size);
  REQUIRE(chunked_buffer.size() == 0);
  REQUIRE(chunked_buffer.nchunks() == nchunks);

  std::cerr << "JOE 8" << std::endl;

  // Verify all chunks are unallocated.
  for (size_t i = 0; i < chunked_buffer.nchunks(); ++i) {
    std::cerr << "JOE 9" << std::endl;
    void* chunk_buffer;
    REQUIRE(chunked_buffer.internal_buffer(i, &chunk_buffer).ok());
    REQUIRE(!chunk_buffer);
  }

  std::cerr << "JOE 10" << std::endl;

  // Write the entire buffer. This will allocate all of the chunks.
  write_offset = 0;
  REQUIRE(chunked_buffer.write(write_buffer, buffer_size, write_offset).ok());

  std::cerr << "JOE 11" << std::endl;
}

TEST_CASE(
    "ChunkedBuffer: Test discrete, fixed size IO 16",
    "[ChunkedBuffer][discrete_fixed_io]") {
  std::cerr << "JOE 1" << std::endl;

  // Instantiate a test ChunkedBuffer.
  ChunkedBuffer chunked_buffer;

  std::cerr << "JOE 2" << std::endl;

  // Create a buffer to write to the test ChunkedBuffer.
  const int64_t buffer_size = 1024 * 1024 * 3;
  uint64_t* const write_buffer = static_cast<uint64_t*>(malloc(buffer_size));
  const uint64_t buffer_len = buffer_size / sizeof(uint64_t);
  for (uint64_t i = 0; i < buffer_len; ++i) {
    write_buffer[i] = i;
  }

  std::cerr << "JOE 3" << std::endl;

  // Attempt a write before initializing the test ChunkedBuffer.
  uint64_t write_offset = 0;
  REQUIRE(!chunked_buffer.write(write_buffer, buffer_size, write_offset).ok());

  std::cerr << "JOE 4" << std::endl;

  // Attempt a read before initializing the test ChunkedBuffer.
  uint64_t read_offset = 0;
  uint64_t read_buffer[buffer_len];
  REQUIRE(!chunked_buffer.read(read_buffer, buffer_size, read_offset).ok());

  std::cerr << "JOE 5" << std::endl;

  // Attempt an alloc before initializing the test ChunkedBuffer.
  size_t chunk_idx = 0;
  void* chunk_buffer = nullptr;
  REQUIRE(!chunked_buffer.alloc_discrete(chunk_idx, &chunk_buffer).ok());
  REQUIRE(!chunk_buffer);

  std::cerr << "JOE 6" << std::endl;

  // Attempt a set before initializing the test ChunkedBuffer.
  chunk_buffer = nullptr;
  REQUIRE(!chunked_buffer.set_contigious(chunk_buffer).ok());
  REQUIRE(!chunk_buffer);

  std::cerr << "JOE 7" << std::endl;

  // Initialize the ChunkedBuffer.
  REQUIRE(buffer_size % sizeof(uint64_t) == 0);
  const size_t chunk_size = 1024 * 100;
  REQUIRE(buffer_size % chunk_size != 0);
  const size_t nchunks = (buffer_size / chunk_size) + 1;
  const size_t last_chunk_size = buffer_size % chunk_size;
  REQUIRE(chunk_size != last_chunk_size);
  chunked_buffer.init_fixed_size(
      ChunkedBuffer::BufferAddressing::DISCRETE, buffer_size, chunk_size);
  REQUIRE(chunked_buffer.capacity() == buffer_size);
  REQUIRE(chunked_buffer.size() == 0);
  REQUIRE(chunked_buffer.nchunks() == nchunks);

  std::cerr << "JOE 8" << std::endl;

  // Verify all chunks are unallocated.
  for (size_t i = 0; i < chunked_buffer.nchunks(); ++i) {
    std::cerr << "JOE 9" << std::endl;
    void* chunk_buffer;
    REQUIRE(chunked_buffer.internal_buffer(i, &chunk_buffer).ok());
    REQUIRE(!chunk_buffer);
  }

  std::cerr << "JOE 10" << std::endl;

  // Write the entire buffer. This will allocate all of the chunks.
  write_offset = 0;
  REQUIRE(chunked_buffer.write(write_buffer, buffer_size, write_offset).ok());

  std::cerr << "JOE 11" << std::endl;

  // Verify all chunks are allocated and that they do not overlap
  // 'buffer' because they have been deep-copied.
  for (size_t i = 0; i < chunked_buffer.nchunks(); ++i) {
    std::cerr << "JOE 12" << std::endl;
    uint32_t internal_size = 0;
    REQUIRE(chunked_buffer.internal_buffer_size(i, &internal_size).ok());
    if (i < chunked_buffer.nchunks() - 1) {
      REQUIRE(internal_size == chunk_size);
    } else {
      REQUIRE(internal_size == last_chunk_size);
    }

    std::cerr << "JOE 13" << std::endl;

    REQUIRE(chunked_buffer.internal_buffer(i, &chunk_buffer).ok());
    REQUIRE(chunk_buffer);
    REQUIRE(chunk_buffer != write_buffer);
    if (chunk_buffer < write_buffer) {
      std::cerr << "JOE 14" << std::endl;
      REQUIRE(
          static_cast<char*>(chunk_buffer) + chunk_size <=
          reinterpret_cast<char*>(write_buffer));
    } else {
      std::cerr << "JOE 15" << std::endl;
      REQUIRE(
          reinterpret_cast<char*>(write_buffer) + buffer_size <=
          static_cast<char*>(chunk_buffer));
    }
  }
  std::cerr << "JOE 16" << std::endl;
}

TEST_CASE(
    "ChunkedBuffer: Test discrete, fixed size IO 17",
    "[ChunkedBuffer][discrete_fixed_io]") {
  std::cerr << "JOE 1" << std::endl;

  // Instantiate a test ChunkedBuffer.
  ChunkedBuffer chunked_buffer;

  std::cerr << "JOE 2" << std::endl;

  // Create a buffer to write to the test ChunkedBuffer.
  const int64_t buffer_size = 1024 * 1024 * 3;
  uint64_t* const write_buffer = static_cast<uint64_t*>(malloc(buffer_size));
  const uint64_t buffer_len = buffer_size / sizeof(uint64_t);
  for (uint64_t i = 0; i < buffer_len; ++i) {
    write_buffer[i] = i;
  }

  std::cerr << "JOE 3" << std::endl;

  // Attempt a write before initializing the test ChunkedBuffer.
  uint64_t write_offset = 0;
  REQUIRE(!chunked_buffer.write(write_buffer, buffer_size, write_offset).ok());

  std::cerr << "JOE 4" << std::endl;

  // Attempt a read before initializing the test ChunkedBuffer.
  uint64_t read_offset = 0;
  uint64_t read_buffer[buffer_len];
  REQUIRE(!chunked_buffer.read(read_buffer, buffer_size, read_offset).ok());

  std::cerr << "JOE 5" << std::endl;

  // Attempt an alloc before initializing the test ChunkedBuffer.
  size_t chunk_idx = 0;
  void* chunk_buffer = nullptr;
  REQUIRE(!chunked_buffer.alloc_discrete(chunk_idx, &chunk_buffer).ok());
  REQUIRE(!chunk_buffer);

  std::cerr << "JOE 6" << std::endl;

  // Attempt a set before initializing the test ChunkedBuffer.
  chunk_buffer = nullptr;
  REQUIRE(!chunked_buffer.set_contigious(chunk_buffer).ok());
  REQUIRE(!chunk_buffer);

  std::cerr << "JOE 7" << std::endl;

  // Initialize the ChunkedBuffer.
  REQUIRE(buffer_size % sizeof(uint64_t) == 0);
  const size_t chunk_size = 1024 * 100;
  REQUIRE(buffer_size % chunk_size != 0);
  const size_t nchunks = (buffer_size / chunk_size) + 1;
  const size_t last_chunk_size = buffer_size % chunk_size;
  REQUIRE(chunk_size != last_chunk_size);
  chunked_buffer.init_fixed_size(
      ChunkedBuffer::BufferAddressing::DISCRETE, buffer_size, chunk_size);
  REQUIRE(chunked_buffer.capacity() == buffer_size);
  REQUIRE(chunked_buffer.size() == 0);
  REQUIRE(chunked_buffer.nchunks() == nchunks);

  std::cerr << "JOE 8" << std::endl;

  // Verify all chunks are unallocated.
  for (size_t i = 0; i < chunked_buffer.nchunks(); ++i) {
    std::cerr << "JOE 9" << std::endl;
    void* chunk_buffer;
    REQUIRE(chunked_buffer.internal_buffer(i, &chunk_buffer).ok());
    REQUIRE(!chunk_buffer);
  }

  std::cerr << "JOE 10" << std::endl;

  // Write the entire buffer. This will allocate all of the chunks.
  write_offset = 0;
  REQUIRE(chunked_buffer.write(write_buffer, buffer_size, write_offset).ok());

  std::cerr << "JOE 11" << std::endl;

  // Verify all chunks are allocated and that they do not overlap
  // 'buffer' because they have been deep-copied.
  for (size_t i = 0; i < chunked_buffer.nchunks(); ++i) {
    std::cerr << "JOE 12" << std::endl;
    uint32_t internal_size = 0;
    REQUIRE(chunked_buffer.internal_buffer_size(i, &internal_size).ok());
    if (i < chunked_buffer.nchunks() - 1) {
      REQUIRE(internal_size == chunk_size);
    } else {
      REQUIRE(internal_size == last_chunk_size);
    }

    std::cerr << "JOE 13" << std::endl;

    REQUIRE(chunked_buffer.internal_buffer(i, &chunk_buffer).ok());
    REQUIRE(chunk_buffer);
    REQUIRE(chunk_buffer != write_buffer);
    if (chunk_buffer < write_buffer) {
      std::cerr << "JOE 14" << std::endl;
      REQUIRE(
          static_cast<char*>(chunk_buffer) + chunk_size <=
          reinterpret_cast<char*>(write_buffer));
    } else {
      std::cerr << "JOE 15" << std::endl;
      REQUIRE(
          reinterpret_cast<char*>(write_buffer) + buffer_size <=
          static_cast<char*>(chunk_buffer));
    }
  }
  std::cerr << "JOE 16" << std::endl;
  // Read the third element, this will be of value '2'.
  read_offset = 2 * sizeof(uint64_t);
  uint64_t two = 0;
  REQUIRE(chunked_buffer.read(&two, sizeof(uint64_t), read_offset).ok());
  REQUIRE(two == 2);

  std::cerr << "JOE 17" << std::endl;
}

TEST_CASE(
    "ChunkedBuffer: Test discrete, fixed size IO 18",
    "[ChunkedBuffer][discrete_fixed_io]") {
  std::cerr << "JOE 1" << std::endl;

  // Instantiate a test ChunkedBuffer.
  ChunkedBuffer chunked_buffer;

  std::cerr << "JOE 2" << std::endl;

  // Create a buffer to write to the test ChunkedBuffer.
  const int64_t buffer_size = 1024 * 1024 * 3;
  uint64_t* const write_buffer = static_cast<uint64_t*>(malloc(buffer_size));
  const uint64_t buffer_len = buffer_size / sizeof(uint64_t);
  for (uint64_t i = 0; i < buffer_len; ++i) {
    write_buffer[i] = i;
  }

  std::cerr << "JOE 3" << std::endl;

  // Attempt a write before initializing the test ChunkedBuffer.
  uint64_t write_offset = 0;
  REQUIRE(!chunked_buffer.write(write_buffer, buffer_size, write_offset).ok());

  std::cerr << "JOE 4" << std::endl;

  // Attempt a read before initializing the test ChunkedBuffer.
  uint64_t read_offset = 0;
  uint64_t read_buffer[buffer_len];
  REQUIRE(!chunked_buffer.read(read_buffer, buffer_size, read_offset).ok());

  std::cerr << "JOE 5" << std::endl;

  // Attempt an alloc before initializing the test ChunkedBuffer.
  size_t chunk_idx = 0;
  void* chunk_buffer = nullptr;
  REQUIRE(!chunked_buffer.alloc_discrete(chunk_idx, &chunk_buffer).ok());
  REQUIRE(!chunk_buffer);

  std::cerr << "JOE 6" << std::endl;

  // Attempt a set before initializing the test ChunkedBuffer.
  chunk_buffer = nullptr;
  REQUIRE(!chunked_buffer.set_contigious(chunk_buffer).ok());
  REQUIRE(!chunk_buffer);

  std::cerr << "JOE 7" << std::endl;

  // Initialize the ChunkedBuffer.
  REQUIRE(buffer_size % sizeof(uint64_t) == 0);
  const size_t chunk_size = 1024 * 100;
  REQUIRE(buffer_size % chunk_size != 0);
  const size_t nchunks = (buffer_size / chunk_size) + 1;
  const size_t last_chunk_size = buffer_size % chunk_size;
  REQUIRE(chunk_size != last_chunk_size);
  chunked_buffer.init_fixed_size(
      ChunkedBuffer::BufferAddressing::DISCRETE, buffer_size, chunk_size);
  REQUIRE(chunked_buffer.capacity() == buffer_size);
  REQUIRE(chunked_buffer.size() == 0);
  REQUIRE(chunked_buffer.nchunks() == nchunks);

  std::cerr << "JOE 8" << std::endl;

  // Verify all chunks are unallocated.
  for (size_t i = 0; i < chunked_buffer.nchunks(); ++i) {
    std::cerr << "JOE 9" << std::endl;
    void* chunk_buffer;
    REQUIRE(chunked_buffer.internal_buffer(i, &chunk_buffer).ok());
    REQUIRE(!chunk_buffer);
  }

  std::cerr << "JOE 10" << std::endl;

  // Write the entire buffer. This will allocate all of the chunks.
  write_offset = 0;
  REQUIRE(chunked_buffer.write(write_buffer, buffer_size, write_offset).ok());

  std::cerr << "JOE 11" << std::endl;

  // Verify all chunks are allocated and that they do not overlap
  // 'buffer' because they have been deep-copied.
  for (size_t i = 0; i < chunked_buffer.nchunks(); ++i) {
    std::cerr << "JOE 12" << std::endl;
    uint32_t internal_size = 0;
    REQUIRE(chunked_buffer.internal_buffer_size(i, &internal_size).ok());
    if (i < chunked_buffer.nchunks() - 1) {
      REQUIRE(internal_size == chunk_size);
    } else {
      REQUIRE(internal_size == last_chunk_size);
    }

    std::cerr << "JOE 13" << std::endl;

    REQUIRE(chunked_buffer.internal_buffer(i, &chunk_buffer).ok());
    REQUIRE(chunk_buffer);
    REQUIRE(chunk_buffer != write_buffer);
    if (chunk_buffer < write_buffer) {
      std::cerr << "JOE 14" << std::endl;
      REQUIRE(
          static_cast<char*>(chunk_buffer) + chunk_size <=
          reinterpret_cast<char*>(write_buffer));
    } else {
      std::cerr << "JOE 15" << std::endl;
      REQUIRE(
          reinterpret_cast<char*>(write_buffer) + buffer_size <=
          static_cast<char*>(chunk_buffer));
    }
  }
  std::cerr << "JOE 16" << std::endl;
  // Read the third element, this will be of value '2'.
  read_offset = 2 * sizeof(uint64_t);
  uint64_t two = 0;
  REQUIRE(chunked_buffer.read(&two, sizeof(uint64_t), read_offset).ok());
  REQUIRE(two == 2);

  std::cerr << "JOE 17" << std::endl;

  // Read the 10th element, this will be of value '9'.
  read_offset = 9 * sizeof(uint64_t);
  uint64_t nine = 0;
  REQUIRE(chunked_buffer.read(&nine, sizeof(uint64_t), read_offset).ok());
  REQUIRE(nine == 9);

  std::cerr << "JOE 18" << std::endl;
}

TEST_CASE(
    "ChunkedBuffer: Test discrete, fixed size IO 0",
    "[ChunkedBuffer][discrete_fixed_io]") {
  std::cerr << "JOE 1" << std::endl;

  // Instantiate a test ChunkedBuffer.
  ChunkedBuffer chunked_buffer;

  std::cerr << "JOE 2" << std::endl;

  // Create a buffer to write to the test ChunkedBuffer.
  const int64_t buffer_size = 1024 * 1024 * 3;
  uint64_t* const write_buffer = static_cast<uint64_t*>(malloc(buffer_size));
  const uint64_t buffer_len = buffer_size / sizeof(uint64_t);
  for (uint64_t i = 0; i < buffer_len; ++i) {
    write_buffer[i] = i;
  }

  std::cerr << "JOE 3" << std::endl;

  // Attempt a write before initializing the test ChunkedBuffer.
  uint64_t write_offset = 0;
  REQUIRE(!chunked_buffer.write(write_buffer, buffer_size, write_offset).ok());

  std::cerr << "JOE 4" << std::endl;

  // Attempt a read before initializing the test ChunkedBuffer.
  uint64_t read_offset = 0;
  uint64_t read_buffer[buffer_len];
  REQUIRE(!chunked_buffer.read(read_buffer, buffer_size, read_offset).ok());

  std::cerr << "JOE 5" << std::endl;

  // Attempt an alloc before initializing the test ChunkedBuffer.
  size_t chunk_idx = 0;
  void* chunk_buffer = nullptr;
  REQUIRE(!chunked_buffer.alloc_discrete(chunk_idx, &chunk_buffer).ok());
  REQUIRE(!chunk_buffer);

  std::cerr << "JOE 6" << std::endl;

  // Attempt a set before initializing the test ChunkedBuffer.
  chunk_buffer = nullptr;
  REQUIRE(!chunked_buffer.set_contigious(chunk_buffer).ok());
  REQUIRE(!chunk_buffer);

  std::cerr << "JOE 7" << std::endl;

  // Initialize the ChunkedBuffer.
  REQUIRE(buffer_size % sizeof(uint64_t) == 0);
  const size_t chunk_size = 1024 * 100;
  REQUIRE(buffer_size % chunk_size != 0);
  const size_t nchunks = (buffer_size / chunk_size) + 1;
  const size_t last_chunk_size = buffer_size % chunk_size;
  REQUIRE(chunk_size != last_chunk_size);
  chunked_buffer.init_fixed_size(
      ChunkedBuffer::BufferAddressing::DISCRETE, buffer_size, chunk_size);
  REQUIRE(chunked_buffer.capacity() == buffer_size);
  REQUIRE(chunked_buffer.size() == 0);
  REQUIRE(chunked_buffer.nchunks() == nchunks);

  std::cerr << "JOE 8" << std::endl;

  // Verify all chunks are unallocated.
  for (size_t i = 0; i < chunked_buffer.nchunks(); ++i) {
    std::cerr << "JOE 9" << std::endl;
    void* chunk_buffer;
    REQUIRE(chunked_buffer.internal_buffer(i, &chunk_buffer).ok());
    REQUIRE(!chunk_buffer);
  }

  std::cerr << "JOE 10" << std::endl;

  // Write the entire buffer. This will allocate all of the chunks.
  write_offset = 0;
  REQUIRE(chunked_buffer.write(write_buffer, buffer_size, write_offset).ok());

  std::cerr << "JOE 11" << std::endl;

  // Verify all chunks are allocated and that they do not overlap
  // 'buffer' because they have been deep-copied.
  for (size_t i = 0; i < chunked_buffer.nchunks(); ++i) {
    std::cerr << "JOE 12" << std::endl;
    uint32_t internal_size = 0;
    REQUIRE(chunked_buffer.internal_buffer_size(i, &internal_size).ok());
    if (i < chunked_buffer.nchunks() - 1) {
      REQUIRE(internal_size == chunk_size);
    } else {
      REQUIRE(internal_size == last_chunk_size);
    }

    std::cerr << "JOE 13" << std::endl;

    REQUIRE(chunked_buffer.internal_buffer(i, &chunk_buffer).ok());
    REQUIRE(chunk_buffer);
    REQUIRE(chunk_buffer != write_buffer);
    if (chunk_buffer < write_buffer) {
      std::cerr << "JOE 14" << std::endl;
      REQUIRE(
          static_cast<char*>(chunk_buffer) + chunk_size <=
          reinterpret_cast<char*>(write_buffer));
    } else {
      std::cerr << "JOE 15" << std::endl;
      REQUIRE(
          reinterpret_cast<char*>(write_buffer) + buffer_size <=
          static_cast<char*>(chunk_buffer));
    }
  }
  std::cerr << "JOE 16" << std::endl;
  // Read the third element, this will be of value '2'.
  read_offset = 2 * sizeof(uint64_t);
  uint64_t two = 0;
  REQUIRE(chunked_buffer.read(&two, sizeof(uint64_t), read_offset).ok());
  REQUIRE(two == 2);

  std::cerr << "JOE 17" << std::endl;

  // Read the 10th element, this will be of value '9'.
  read_offset = 9 * sizeof(uint64_t);
  uint64_t nine = 0;
  REQUIRE(chunked_buffer.read(&nine, sizeof(uint64_t), read_offset).ok());
  REQUIRE(nine == 9);

  std::cerr << "JOE 18" << std::endl;

  // Read the 100th element, this will be of value '99'.
  read_offset = 99 * sizeof(uint64_t);
  uint64_t ninety_nine = 0;
  REQUIRE(
      chunked_buffer.read(&ninety_nine, sizeof(uint64_t), read_offset).ok());
  REQUIRE(ninety_nine == 99);

  std::cerr << "JOE 19" << std::endl;

  // Overwrite the 100th element with value '900'.
  write_offset = 99 * sizeof(uint64_t);
  uint64_t nine_hundred = 900;
  REQUIRE(
      chunked_buffer.write(&nine_hundred, sizeof(uint64_t), write_offset).ok());

  std::cerr << "JOE 20" << std::endl;

  // Read the 100th element, this will be of value '900'.
  read_offset = 99 * sizeof(uint64_t);
  nine_hundred = 0;
  REQUIRE(
      chunked_buffer.read(&nine_hundred, sizeof(uint64_t), read_offset).ok());
  REQUIRE(nine_hundred == 900);

  std::cerr << "JOE 21" << std::endl;

  // Overwrite the 100th element back to value '99'.
  write_offset = 99 * sizeof(uint64_t);
  ninety_nine = 99;
  REQUIRE(
      chunked_buffer.write(&ninety_nine, sizeof(uint64_t), write_offset).ok());

  std::cerr << "JOE 22" << std::endl;

  // Read the entire written buffer.
  read_offset = 0;
  REQUIRE(chunked_buffer.read(read_buffer, buffer_size, read_offset).ok());
  REQUIRE(memcmp(read_buffer, write_buffer, buffer_size) == 0);

  std::cerr << "JOE 23" << std::endl;

  // Free the chunk buffer, which will free all of the allocated
  // buffers and return chunked_buffer into an uninitialized state.
  chunked_buffer.free();
  REQUIRE(chunked_buffer.size() == 0);
  REQUIRE(chunked_buffer.nchunks() == 0);

  std::cerr << "JOE 24" << std::endl;

  // Reinitialize the chunk buffers.
  chunked_buffer.init_fixed_size(
      ChunkedBuffer::BufferAddressing::DISCRETE, buffer_size, chunk_size);
  REQUIRE(chunked_buffer.capacity() == buffer_size);
  REQUIRE(chunked_buffer.size() == 0);
  REQUIRE(chunked_buffer.nchunks() == nchunks);

  std::cerr << "JOE 25" << std::endl;

  // Verify all chunks are unallocated.
  for (size_t i = 0; i < chunked_buffer.nchunks(); ++i) {
    std::cerr << "JOE 26" << std::endl;
    uint32_t internal_size = 0;
    REQUIRE(chunked_buffer.internal_buffer_size(i, &internal_size).ok());
    REQUIRE(internal_size == 0);

    uint32_t internal_capacity = 0;
    REQUIRE(
        chunked_buffer.internal_buffer_capacity(i, &internal_capacity).ok());
    std::cerr << "JOE 27" << std::endl;
    if (i < chunked_buffer.nchunks() - 1) {
      REQUIRE(internal_capacity == chunk_size);
    } else {
      REQUIRE(internal_capacity == last_chunk_size);
    }
  }

  std::cerr << "JOE 28" << std::endl;

  // Allocate all chunks.
  std::vector<void*> internal_chunked_buffer(chunked_buffer.nchunks());
  for (size_t i = 0; i < chunked_buffer.nchunks(); ++i) {
    REQUIRE(chunked_buffer.alloc_discrete(i, &chunk_buffer).ok());
    REQUIRE(chunk_buffer);
    internal_chunked_buffer[i] = chunk_buffer;
  }

  std::cerr << "JOE 29" << std::endl;

  // Verify all chunks are allocated and that they do not overlap
  // 'buffer' because they have been deep-copied.
  for (size_t i = 0; i < chunked_buffer.nchunks(); ++i) {
    std::cerr << "JOE 30" << std::endl;
    uint32_t internal_size = 0;
    REQUIRE(chunked_buffer.internal_buffer_size(i, &internal_size).ok());
    REQUIRE(internal_size == 0);

    std::cerr << "JOE 31" << std::endl;

    uint32_t internal_capacity = 0;
    REQUIRE(
        chunked_buffer.internal_buffer_capacity(i, &internal_capacity).ok());
    if (i < chunked_buffer.nchunks() - 1) {
      REQUIRE(internal_capacity == chunk_size);
    } else {
      REQUIRE(internal_capacity == last_chunk_size);
    }

    std::cerr << "JOE 32" << std::endl;

    REQUIRE(chunked_buffer.internal_buffer(i, &chunk_buffer).ok());
    REQUIRE(chunk_buffer);
    REQUIRE(chunk_buffer != write_buffer);
    if (chunk_buffer < write_buffer) {
      REQUIRE(
          static_cast<char*>(chunk_buffer) + chunk_size <=
          reinterpret_cast<char*>(write_buffer));
    } else {
      REQUIRE(
          reinterpret_cast<char*>(write_buffer) + buffer_size <=
          static_cast<char*>(chunk_buffer));
    }

    std::cerr << "JOE 33" << std::endl;
  }

  std::cerr << "JOE 34" << std::endl;

  // Write to all chunks.
  write_offset = 0;
  REQUIRE(chunked_buffer.write(write_buffer, buffer_size, write_offset).ok());

  std::cerr << "JOE 35" << std::endl;

  // Read the entire written buffer.
  read_offset = 0;
  REQUIRE(chunked_buffer.read(read_buffer, buffer_size, read_offset).ok());
  REQUIRE(memcmp(read_buffer, write_buffer, buffer_size) == 0);

  std::cerr << "JOE 36" << std::endl;

  // Clear the ChunkedBuffer. This will reset to an uninitialized state
  // but will NOT free the underlying buffers.
  chunked_buffer.clear();
  REQUIRE(chunked_buffer.size() == 0);
  REQUIRE(chunked_buffer.nchunks() == 0);

  std::cerr << "JOE 37" << std::endl;

  // Free the internal buffers to prevent a memory leak.
  for (const auto& internal_buffer : internal_chunked_buffer) {
    free(internal_buffer);
  }

  std::cerr << "JOE 38" << std::endl;
}
