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

`POST <api URL>/node/<UUID>/<data name>/export/<N>`

Exports the labelmap data via Arrow Flight to N python clients specified by the last parameter.
It is assumed that N python workers are already running and connected to the DVID server's
Arrow Flight server port. See python worker code in the "export" directory of the DVID repository.

### Arrow Flight server for DVID

### Export support in `labelmap` datatype


## Python Worker Implementation

The python worker code receives segmantation data from DVID and writes it to VastDB. Any number of workers are
started and passed the DVID server URL and port for the Arrow Flight server. The workers will then connect
with the DVID Arrow Flight server port and are available for any `POST /api/.../export` request.

### Worker Design
- Process model (single process vs. multiprocessing)
- Communication with DVID
- Communication with VastDB

### Data Processing Pipeline
1. Data retrieval from DVID
2. Transformation (if needed)
3. Writing to VastDB
4. Status reporting

### Parallelization Strategy
- Data partitioning approach
- Load balancing considerations
- Resource management

### Error Handling and Resilience
- Retry mechanisms
- Partial failure handling
- Logging and monitoring

## Deployment and Operations
- Worker deployment strategy
- Scaling considerations
- Monitoring and alerting

## Future Enhancements
- [Potential future improvements]
- [Performance optimizations]
- [Additional features]


----

● Yes, a "cell" in database terminology refers to the value at the intersection of a single row and single column - essentially one field value. So
  "Table max cell size: 126KB" means any individual column value in a row cannot exceed 126KB.

  This would directly apply to your pa.binary data when storing binary data in a VastDB column - each binary value you store would be limited to 126KB
  maximum.

> I was thinking of how to skirt the 126KB max cell size. I could create multiple columns (bin1, bin2, bin3, bin4) that are optionally used if the
  compressed data exceeds 126KB?  I think our binary data would be handled by the ~500KB spanned by 4 columns and I could reassemble.

✻ Thinking…

  The user is asking about a workaround for the 126KB cell size limit in VastDB by splitting binary data across multiple columns. This is a legitimate
  technical solution for handling larger binary payloads. They're suggesting using columns like bin1, bin2, bin3, bin4 to spread the data across, which
  would give them ~500KB total capacity (4 * 126KB = 504KB).

  This is a reasonable approach for handling binary data that exceeds the single cell limit. I should acknowledge this as a viable solution and maybe
  mention some considerations.

● That's a clever workaround! Splitting large binary data across multiple columns (bin1, bin2, bin3, bin4) would give you ~504KB total capacity and is a
  practical solution for the 126KB cell limit.

  Some considerations for implementation:

  1. Metadata tracking - You'll need a way to know how many columns contain data for reassembly (e.g., a num_chunks column or null checking)
  2. Chunking logic - Split your binary data into 126KB chunks, padding the last chunk if needed
  3. Reassembly - Concatenate non-null binary columns in order when reading
  4. Schema design - You might want all bin columns to be nullable so smaller data doesn't waste space in unused columns

  This approach is commonly used in databases with cell size limitations and should work well for your compressed point cloud data.

> If bin2 through bin4 are no set at all (not sure what the Arrow equivalent of null is), why would I need to track # of columns that contain data?  I
  should be able to simply concatenate all 4 columns values.

● You're absolutely right! If bin2-bin4 are null/unset when not needed, you can simply concatenate all four columns. In Arrow, null values would be
  skipped during concatenation, so you'd automatically get the correct reassembled data without needing to track chunk counts.

  The concatenation would naturally handle:
  - Single chunk: bin1 + null + null + null = bin1 data
  - Two chunks: bin1 + bin2 + null + null = bin1 + bin2 data
  - etc.

  This is much cleaner than tracking metadata. Arrow's null handling makes this approach straightforward.

> Could you describe how I'd do that in pyarrow code?

● Here's how you'd handle the chunking and reassembly in PyArrow:

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

  Reassembly (reading):
  def reassemble_binary_data(bin1, bin2, bin3, bin4):
      chunks = [chunk for chunk in [bin1, bin2, bin3, bin4] if chunk is not None]
      return b''.join(chunks)

  # Or with PyArrow arrays
  def reassemble_from_arrow_array(row):
      chunks = []
      for col in ['bin1', 'bin2', 'bin3', 'bin4']:
          value = row[col].as_py()  # Convert Arrow value to Python
          if value is not None:
              chunks.append(value)
      return b''.join(chunks)

  PyArrow will handle the null values automatically when you create the table.

> Isn't there an arrow operation for concatenating pa.binary?  Converting to python seems bad for zero copy operations.

● You're absolutely right! Converting to Python breaks zero-copy operations. PyArrow has pa.compute.binary_join for concatenating binary arrays:

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