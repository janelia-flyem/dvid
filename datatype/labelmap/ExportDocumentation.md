# Labelmap (Segmentation) Export System Architecture

## System Summary
This document outlines the architecture for an export system that facilitates efficient data transfer from a DVID `labelmap` instance at a particular UUID to VastDB. The system aims to minimize potential bottlenecks throughout the transfer process. 

### Potential Bottlenecks
- A DVID `labelmap` instance is held within one embedded Badger keyvalue DB. Even though that DB is typically on a NVMe SSD array or disk, there could be limitations in how quickly all `Index` and `Block` data can be read from storage. Both types of data are defined in the DVID package `datatype/common/labels`.
- Label Block data uses bespoke DVID compression as well as a wrapper compression like LZ4 or Gzip. We'll need to uncompress the wrapper to get the bespoke DVID compression, remove an index of supervoxel label IDs, and then repackage each block's index of supervoxel as well as agglomerated (mapped) label IDs together with perhaps a better ZSTD compression of the bespoke DVID compression (minus the label indices).
- VastDB uses Arrow data structures natively so we should minimize the overhead of creating Arrow data and exploit its zero-copy feature as much as possible.
- The modified label block (+ a few additional values) and index data needs to be written in parallel into VastDB, which can handle a lot of parallel writes.


### Overall Architecture
To address the bottlenecks above, the export system is implemented with the following code:
- An Arrow Flight server with its own port is added to DVID, similar to how the existing HTTP and Go RPC servers are positioned within the DVID code base.
- A new `export.go` file within DVID under the `datatype/labelmap` package that implements efficient reading of all label `Index` and `Block` data for a given `labelmap` data instance and version UUID. As this data is read, it's streamed across channels to N goroutines that stream data to N python workers using the Arrow Flight server.
- Each python worker in a cluster connects to the DVID Arrow Flight server on startup. On receiving the label data, it will efficiently write the data to the VastDB storage system.

Now each of these pieces will be described in more detail.

## DVID Go Implementation

Some initial code has already been added providing a handler for the endpoint:

`POST <api URL>/node/<UUID>/<data name>/export`

Exports the labelmap data via Arrow Flight to N python clients specified by the last parameter.
It is assumed that N python workers are already running and connected to the DVID server's
Arrow Flight server port. See python worker code in the "export" directory of the DVID repository.

### Arrow Flight server for DVID

The arrow flight server is started together with the standard HTTP and Go RPC ports. It defaults to
port `8002`, which can be configured in the DVID TOML file.

### Export support in `labelmap` datatype

After a `POST /export` request, DVID creates goroutines that start with  `readBlocks` that does
a range read across all block keys pertinent to the given `labelmap` instance for the given version UUID.
It sends the data in to a chunk channel that distributes the data across workers that determine the block
coordinate, morton code, and the shard id to which that block belongs. Block data is then sent to 
workers that communicate directly with the N python workers via Apache Flight, making sure that blocks
with the same shard id go to the same python worker. In other words, for any unique shard id, blocks that
are assigned to that shard will be delivered to the same python worker.


## Python Worker Implementation

The python worker code receives segmantation data from DVID and writes it to VastDB. Any number of workers are
started and passed the DVID server URL and port for the Arrow Flight server. The workers will then connect
with the DVID Arrow Flight server port and are available for any `POST /api/.../export` request.

### VastDB optimizations: ordering by shard id and morton code.

Ideally, we'd like the VastDB tables to be organized in a manner similar to how we'll be accessing the data.
Since a later stage will create, in parallel, neuroglancer precomputed volume portions by shard id, we
want to add a `sorting_key` prioritizing the shard id and then the morton code when creating a VastDB table.
It's likely we'd also want to use `create_projection()` to also allow easy quering of other properties.

No cell in a VastDB table can exceed 126KB in size. So block data would have to be within 126KB size,
although we could do a workaround by providing multiple columns (e.g., bin1, bin2, bin3, bin4) that are 
optionally used if the compressed data exceeds 126KB?  The binary data for a block would be handled by the 
~500KB spanned by 4 columns and I could reassemble.

Here's how you'd handle the chunking and reassembly in PyArrow:

Chunking (writing):

  import pyarrow as pa

  def chunk_binary_data(data, chunk_size=126*1024):  # 126KB
      chunks = [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]
      # Pad with None to fill 4 columns
      while len(chunks) < 4:
          chunks.append(None)
      return chunks[:4]  # Ensure exactly 4 chunks

  # Usage
  binary_data = b"your_compressed_data_here"
  bin1, bin2, bin3, bin4 = chunk_binary_data(binary_data)

Reassembly with zero-copy:

  import pyarrow.compute as pc

  def reassemble_binary_data(table):
      # Get the binary columns as arrays
      bin_arrays = [table['bin1'], table['bin2'], table['bin3'], table['bin4']]

      # Filter out null arrays (optional - binary_join handles nulls)
      non_null_arrays = [arr for arr in bin_arrays if not arr.null_count == len(arr)]

      # Concatenate using Arrow compute
      if len(non_null_arrays) == 1:
          return non_null_arrays[0]
      else:
          return pc.binary_join(non_null_arrays, separator=b'')

Or more directly:
  # Concatenate all 4 columns (nulls are handled automatically)
  result = pc.binary_join([table['bin1'], table['bin2'], table['bin3'], table['bin4']],
                         separator=b'')

This keeps everything in Arrow format and maintains zero-copy performance.