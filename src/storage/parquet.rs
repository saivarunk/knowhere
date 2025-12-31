use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;

use super::table::{Column, DataType, Row, Schema, Table, Value};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ParquetError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Invalid Parquet file: {0}")]
    InvalidFormat(String),
    #[error("Unsupported feature: {0}")]
    Unsupported(String),
    #[error("Decompression error: {0}")]
    Decompression(String),
}

// Parquet types
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(i32)]
enum ParquetType {
    Boolean = 0,
    Int32 = 1,
    Int64 = 2,
    Int96 = 3,
    Float = 4,
    Double = 5,
    ByteArray = 6,
    FixedLenByteArray = 7,
}

impl TryFrom<i32> for ParquetType {
    type Error = ParquetError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ParquetType::Boolean),
            1 => Ok(ParquetType::Int32),
            2 => Ok(ParquetType::Int64),
            3 => Ok(ParquetType::Int96),
            4 => Ok(ParquetType::Float),
            5 => Ok(ParquetType::Double),
            6 => Ok(ParquetType::ByteArray),
            7 => Ok(ParquetType::FixedLenByteArray),
            _ => Err(ParquetError::InvalidFormat(format!(
                "Unknown parquet type: {}",
                value
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(i32)]
enum Encoding {
    Plain = 0,
    PlainDictionary = 2,
    Rle = 3,
    BitPacked = 4,
    DeltaBinaryPacked = 5,
    DeltaLengthByteArray = 6,
    DeltaByteArray = 7,
    RleDictionary = 8,
}

impl TryFrom<i32> for Encoding {
    type Error = ParquetError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Encoding::Plain),
            2 => Ok(Encoding::PlainDictionary),
            3 => Ok(Encoding::Rle),
            4 => Ok(Encoding::BitPacked),
            5 => Ok(Encoding::DeltaBinaryPacked),
            6 => Ok(Encoding::DeltaLengthByteArray),
            7 => Ok(Encoding::DeltaByteArray),
            8 => Ok(Encoding::RleDictionary),
            _ => Err(ParquetError::Unsupported(format!(
                "Unsupported encoding: {}",
                value
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(i32)]
enum CompressionCodec {
    Uncompressed = 0,
    Snappy = 1,
    Gzip = 2,
    Lzo = 3,
    Brotli = 4,
    Lz4 = 5,
    Zstd = 6,
}

impl TryFrom<i32> for CompressionCodec {
    type Error = ParquetError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(CompressionCodec::Uncompressed),
            1 => Ok(CompressionCodec::Snappy),
            2 => Ok(CompressionCodec::Gzip),
            3 => Ok(CompressionCodec::Lzo),
            4 => Ok(CompressionCodec::Brotli),
            5 => Ok(CompressionCodec::Lz4),
            6 => Ok(CompressionCodec::Zstd),
            _ => Err(ParquetError::Unsupported(format!(
                "Unsupported compression: {}",
                value
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(i32)]
enum PageType {
    DataPage = 0,
    IndexPage = 1,
    DictionaryPage = 2,
    DataPageV2 = 3,
}

impl TryFrom<i32> for PageType {
    type Error = ParquetError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(PageType::DataPage),
            1 => Ok(PageType::IndexPage),
            2 => Ok(PageType::DictionaryPage),
            3 => Ok(PageType::DataPageV2),
            _ => Err(ParquetError::InvalidFormat(format!(
                "Unknown page type: {}",
                value
            ))),
        }
    }
}

#[derive(Debug)]
struct SchemaElement {
    name: String,
    parquet_type: Option<ParquetType>,
    num_children: i32,
    type_length: Option<i32>,
}

#[derive(Debug)]
struct ColumnChunk {
    file_offset: i64,
    meta_data: ColumnMetaData,
}

#[derive(Debug)]
struct ColumnMetaData {
    parquet_type: ParquetType,
    encodings: Vec<Encoding>,
    path_in_schema: Vec<String>,
    codec: CompressionCodec,
    num_values: i64,
    total_uncompressed_size: i64,
    total_compressed_size: i64,
    data_page_offset: i64,
    dictionary_page_offset: Option<i64>,
}

#[derive(Debug)]
struct RowGroup {
    columns: Vec<ColumnChunk>,
    total_byte_size: i64,
    num_rows: i64,
}

#[derive(Debug)]
struct FileMetaData {
    version: i32,
    schema: Vec<SchemaElement>,
    num_rows: i64,
    row_groups: Vec<RowGroup>,
}

pub struct ParquetReader;

impl Default for ParquetReader {
    fn default() -> Self {
        Self::new()
    }
}

impl ParquetReader {
    pub fn new() -> Self {
        Self
    }

    pub fn read_file(&self, path: &Path) -> Result<Table, ParquetError> {
        let file = File::open(path)?;
        let table_name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("table")
            .to_string();

        let mut reader = BufReader::new(file);
        self.read_from_reader(&mut reader, &table_name)
    }

    fn read_from_reader<R: Read + Seek>(
        &self,
        reader: &mut R,
        table_name: &str,
    ) -> Result<Table, ParquetError> {
        // Verify magic bytes at start
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        if &magic != b"PAR1" {
            return Err(ParquetError::InvalidFormat(
                "Invalid magic bytes at start".into(),
            ));
        }

        // Read footer length and magic at end
        reader.seek(SeekFrom::End(-8))?;
        let mut footer_buf = [0u8; 8];
        reader.read_exact(&mut footer_buf)?;

        let footer_length = i32::from_le_bytes([
            footer_buf[0],
            footer_buf[1],
            footer_buf[2],
            footer_buf[3],
        ]);

        if &footer_buf[4..8] != b"PAR1" {
            return Err(ParquetError::InvalidFormat(
                "Invalid magic bytes at end".into(),
            ));
        }

        // Read footer (Thrift-encoded FileMetaData)
        reader.seek(SeekFrom::End(-8 - footer_length as i64))?;
        let mut footer_data = vec![0u8; footer_length as usize];
        reader.read_exact(&mut footer_data)?;

        let metadata = self.parse_file_metadata(&footer_data)?;

        // Build schema
        let columns = self.build_schema(&metadata.schema)?;
        let schema = Schema::new(columns);

        // Read data from row groups
        let mut rows = Vec::new();
        for row_group in &metadata.row_groups {
            let group_rows = self.read_row_group(reader, row_group, &metadata.schema)?;
            rows.extend(group_rows);
        }

        Ok(Table::with_rows(table_name, schema, rows))
    }

    fn parse_file_metadata(&self, data: &[u8]) -> Result<FileMetaData, ParquetError> {
        let mut decoder = ThriftDecoder::new(data);

        let mut version = 0i32;
        let mut schema = Vec::new();
        let mut num_rows = 0i64;
        let mut row_groups = Vec::new();

        while let Some((field_id, field_type)) = decoder.read_field_header()? {
            match field_id {
                1 => version = decoder.read_i32()?,
                2 => schema = self.parse_schema_elements(&mut decoder)?,
                3 => num_rows = decoder.read_i64()?,
                4 => row_groups = self.parse_row_groups(&mut decoder)?,
                _ => decoder.skip_field(field_type)?,
            }
        }

        Ok(FileMetaData {
            version,
            schema,
            num_rows,
            row_groups,
        })
    }

    fn parse_schema_elements(
        &self,
        decoder: &mut ThriftDecoder,
    ) -> Result<Vec<SchemaElement>, ParquetError> {
        let list_header = decoder.read_list_header()?;
        let mut elements = Vec::with_capacity(list_header.size as usize);

        for _ in 0..list_header.size {
            elements.push(self.parse_schema_element(decoder)?);
        }

        Ok(elements)
    }

    fn parse_schema_element(
        &self,
        decoder: &mut ThriftDecoder,
    ) -> Result<SchemaElement, ParquetError> {
        let mut parquet_type = None;
        let mut name = String::new();
        let mut num_children = 0i32;
        let mut type_length = None;

        while let Some((field_id, field_type)) = decoder.read_field_header()? {
            match field_id {
                1 => parquet_type = Some(ParquetType::try_from(decoder.read_i32()?)?),
                4 => name = decoder.read_string()?,
                5 => num_children = decoder.read_i32()?,
                6 => type_length = Some(decoder.read_i32()?),
                _ => decoder.skip_field(field_type)?,
            }
        }

        Ok(SchemaElement {
            name,
            parquet_type,
            num_children,
            type_length,
        })
    }

    fn parse_row_groups(
        &self,
        decoder: &mut ThriftDecoder,
    ) -> Result<Vec<RowGroup>, ParquetError> {
        let list_header = decoder.read_list_header()?;
        let mut row_groups = Vec::with_capacity(list_header.size as usize);

        for _ in 0..list_header.size {
            row_groups.push(self.parse_row_group(decoder)?);
        }

        Ok(row_groups)
    }

    fn parse_row_group(&self, decoder: &mut ThriftDecoder) -> Result<RowGroup, ParquetError> {
        let mut columns = Vec::new();
        let mut total_byte_size = 0i64;
        let mut num_rows = 0i64;

        while let Some((field_id, field_type)) = decoder.read_field_header()? {
            match field_id {
                1 => columns = self.parse_column_chunks(decoder)?,
                2 => total_byte_size = decoder.read_i64()?,
                3 => num_rows = decoder.read_i64()?,
                _ => decoder.skip_field(field_type)?,
            }
        }

        Ok(RowGroup {
            columns,
            total_byte_size,
            num_rows,
        })
    }

    fn parse_column_chunks(
        &self,
        decoder: &mut ThriftDecoder,
    ) -> Result<Vec<ColumnChunk>, ParquetError> {
        let list_header = decoder.read_list_header()?;
        let mut chunks = Vec::with_capacity(list_header.size as usize);

        for _ in 0..list_header.size {
            chunks.push(self.parse_column_chunk(decoder)?);
        }

        Ok(chunks)
    }

    fn parse_column_chunk(&self, decoder: &mut ThriftDecoder) -> Result<ColumnChunk, ParquetError> {
        let mut file_offset = 0i64;
        let mut meta_data = None;

        while let Some((field_id, field_type)) = decoder.read_field_header()? {
            match field_id {
                2 => file_offset = decoder.read_i64()?,
                3 => meta_data = Some(self.parse_column_metadata(decoder)?),
                _ => decoder.skip_field(field_type)?,
            }
        }

        Ok(ColumnChunk {
            file_offset,
            meta_data: meta_data.ok_or_else(|| {
                ParquetError::InvalidFormat("Missing column metadata".into())
            })?,
        })
    }

    fn parse_column_metadata(
        &self,
        decoder: &mut ThriftDecoder,
    ) -> Result<ColumnMetaData, ParquetError> {
        let mut parquet_type = ParquetType::Int32;
        let mut encodings = Vec::new();
        let mut path_in_schema = Vec::new();
        let mut codec = CompressionCodec::Uncompressed;
        let mut num_values = 0i64;
        let mut total_uncompressed_size = 0i64;
        let mut total_compressed_size = 0i64;
        let mut data_page_offset = 0i64;
        let mut dictionary_page_offset = None;

        while let Some((field_id, field_type)) = decoder.read_field_header()? {
            match field_id {
                1 => parquet_type = ParquetType::try_from(decoder.read_i32()?)?,
                2 => encodings = self.parse_encodings(decoder)?,
                3 => path_in_schema = self.parse_string_list(decoder)?,
                4 => codec = CompressionCodec::try_from(decoder.read_i32()?)?,
                5 => num_values = decoder.read_i64()?,
                6 => total_uncompressed_size = decoder.read_i64()?,
                7 => total_compressed_size = decoder.read_i64()?,
                9 => data_page_offset = decoder.read_i64()?,
                11 => dictionary_page_offset = Some(decoder.read_i64()?),
                _ => decoder.skip_field(field_type)?,
            }
        }

        Ok(ColumnMetaData {
            parquet_type,
            encodings,
            path_in_schema,
            codec,
            num_values,
            total_uncompressed_size,
            total_compressed_size,
            data_page_offset,
            dictionary_page_offset,
        })
    }

    fn parse_encodings(&self, decoder: &mut ThriftDecoder) -> Result<Vec<Encoding>, ParquetError> {
        let list_header = decoder.read_list_header()?;
        let mut encodings = Vec::with_capacity(list_header.size as usize);

        for _ in 0..list_header.size {
            encodings.push(Encoding::try_from(decoder.read_i32()?)?);
        }

        Ok(encodings)
    }

    fn parse_string_list(&self, decoder: &mut ThriftDecoder) -> Result<Vec<String>, ParquetError> {
        let list_header = decoder.read_list_header()?;
        let mut strings = Vec::with_capacity(list_header.size as usize);

        for _ in 0..list_header.size {
            strings.push(decoder.read_string()?);
        }

        Ok(strings)
    }

    fn build_schema(&self, elements: &[SchemaElement]) -> Result<Vec<Column>, ParquetError> {
        let mut columns = Vec::new();

        // Skip the root element (first one with children)
        for element in elements.iter().skip(1) {
            if element.num_children == 0 {
                // Leaf column
                let data_type = match element.parquet_type {
                    Some(ParquetType::Boolean) => DataType::Boolean,
                    Some(ParquetType::Int32) | Some(ParquetType::Int64) => DataType::Integer,
                    Some(ParquetType::Float) | Some(ParquetType::Double) => DataType::Float,
                    Some(ParquetType::ByteArray) | Some(ParquetType::FixedLenByteArray) => {
                        DataType::String
                    }
                    _ => DataType::String,
                };
                columns.push(Column::new(&element.name, data_type));
            }
        }

        Ok(columns)
    }

    fn read_row_group<R: Read + Seek>(
        &self,
        reader: &mut R,
        row_group: &RowGroup,
        schema: &[SchemaElement],
    ) -> Result<Vec<Row>, ParquetError> {
        let num_rows = row_group.num_rows as usize;
        let num_cols = row_group.columns.len();

        // Read each column
        let mut column_values: Vec<Vec<Value>> = Vec::with_capacity(num_cols);

        for (col_idx, chunk) in row_group.columns.iter().enumerate() {
            let col_schema = &schema[col_idx + 1]; // Skip root
            let values = self.read_column_chunk(reader, chunk, col_schema)?;
            column_values.push(values);
        }

        // Transpose column-major to row-major
        let mut rows = Vec::with_capacity(num_rows);
        for row_idx in 0..num_rows {
            let values: Vec<Value> = column_values
                .iter()
                .map(|col| col.get(row_idx).cloned().unwrap_or(Value::Null))
                .collect();
            rows.push(Row::new(values));
        }

        Ok(rows)
    }

    fn read_column_chunk<R: Read + Seek>(
        &self,
        reader: &mut R,
        chunk: &ColumnChunk,
        _schema: &SchemaElement,
    ) -> Result<Vec<Value>, ParquetError> {
        let mut values = Vec::new();
        let mut dictionary: Option<Vec<Value>> = None;

        // Read dictionary page if present
        if let Some(dict_offset) = chunk.meta_data.dictionary_page_offset {
            reader.seek(SeekFrom::Start(dict_offset as u64))?;
            dictionary = Some(self.read_dictionary_page(reader, &chunk.meta_data)?);
        }

        // Read data pages
        reader.seek(SeekFrom::Start(chunk.meta_data.data_page_offset as u64))?;

        let mut values_read = 0i64;
        while values_read < chunk.meta_data.num_values {
            let (page_values, count) =
                self.read_data_page(reader, &chunk.meta_data, dictionary.as_ref())?;
            values.extend(page_values);
            values_read += count;
        }

        Ok(values)
    }

    fn read_dictionary_page<R: Read>(
        &self,
        reader: &mut R,
        meta: &ColumnMetaData,
    ) -> Result<Vec<Value>, ParquetError> {
        let header = self.read_page_header(reader)?;

        let compressed_size = header.compressed_page_size as usize;
        let mut compressed_data = vec![0u8; compressed_size];
        reader.read_exact(&mut compressed_data)?;

        let data = self.decompress(&compressed_data, header.uncompressed_page_size as usize, meta.codec)?;

        self.decode_plain_values(&data, meta.parquet_type, header.num_values as usize)
    }

    fn read_data_page<R: Read>(
        &self,
        reader: &mut R,
        meta: &ColumnMetaData,
        dictionary: Option<&Vec<Value>>,
    ) -> Result<(Vec<Value>, i64), ParquetError> {
        let header = self.read_page_header(reader)?;

        let compressed_size = header.compressed_page_size as usize;
        let mut compressed_data = vec![0u8; compressed_size];
        reader.read_exact(&mut compressed_data)?;

        let data = self.decompress(&compressed_data, header.uncompressed_page_size as usize, meta.codec)?;

        let num_values = header.num_values as usize;

        // Check encoding
        let values = if let Some(dict) = dictionary {
            // Dictionary encoded
            self.decode_dictionary_values(&data, dict, num_values)?
        } else {
            // Plain encoded
            self.decode_plain_values(&data, meta.parquet_type, num_values)?
        };

        Ok((values, header.num_values as i64))
    }

    fn read_page_header<R: Read>(&self, reader: &mut R) -> Result<PageHeader, ParquetError> {
        // Read Thrift-encoded page header
        let mut buf = vec![0u8; 1024]; // Should be enough for header
        let start_pos = 0;

        // Read byte by byte to find struct end
        let mut header_size = 0;
        for i in 0..buf.len() {
            reader.read_exact(&mut buf[i..i+1])?;
            header_size = i + 1;

            // Simple heuristic: headers are typically small
            // Try to parse after each byte
            if header_size >= 8 {
                let mut decoder = ThriftDecoder::new(&buf[..header_size]);
                if let Ok(header) = self.try_parse_page_header(&mut decoder) {
                    if decoder.position() <= header_size {
                        // Seek back any extra bytes we read
                        // Note: this is a simplification; real implementation would be more careful
                        return Ok(header);
                    }
                }
            }
        }

        // Fallback: try to parse what we have
        let mut decoder = ThriftDecoder::new(&buf[start_pos..header_size]);
        self.try_parse_page_header(&mut decoder)
    }

    fn try_parse_page_header(&self, decoder: &mut ThriftDecoder) -> Result<PageHeader, ParquetError> {
        let mut page_type = PageType::DataPage;
        let mut uncompressed_page_size = 0i32;
        let mut compressed_page_size = 0i32;
        let mut num_values = 0i32;

        while let Some((field_id, field_type)) = decoder.read_field_header()? {
            match field_id {
                1 => page_type = PageType::try_from(decoder.read_i32()?)?,
                2 => uncompressed_page_size = decoder.read_i32()?,
                3 => compressed_page_size = decoder.read_i32()?,
                5 => {
                    // DataPageHeader - read nested struct
                    while let Some((inner_id, inner_type)) = decoder.read_field_header()? {
                        match inner_id {
                            1 => num_values = decoder.read_i32()?,
                            _ => decoder.skip_field(inner_type)?,
                        }
                    }
                }
                7 => {
                    // DataPageHeaderV2
                    while let Some((inner_id, inner_type)) = decoder.read_field_header()? {
                        match inner_id {
                            1 => num_values = decoder.read_i32()?,
                            _ => decoder.skip_field(inner_type)?,
                        }
                    }
                }
                _ => decoder.skip_field(field_type)?,
            }
        }

        Ok(PageHeader {
            page_type,
            uncompressed_page_size,
            compressed_page_size,
            num_values,
        })
    }

    fn decompress(
        &self,
        data: &[u8],
        uncompressed_size: usize,
        codec: CompressionCodec,
    ) -> Result<Vec<u8>, ParquetError> {
        match codec {
            CompressionCodec::Uncompressed => Ok(data.to_vec()),
            CompressionCodec::Snappy => {
                let mut decoder = snap::raw::Decoder::new();
                decoder
                    .decompress_vec(data)
                    .map_err(|e| ParquetError::Decompression(e.to_string()))
            }
            CompressionCodec::Gzip => {
                use flate2::read::GzDecoder;
                let mut decoder = GzDecoder::new(data);
                let mut decompressed = Vec::with_capacity(uncompressed_size);
                decoder
                    .read_to_end(&mut decompressed)
                    .map_err(|e| ParquetError::Decompression(e.to_string()))?;
                Ok(decompressed)
            }
            _ => Err(ParquetError::Unsupported(format!(
                "Compression codec {:?}",
                codec
            ))),
        }
    }

    fn decode_plain_values(
        &self,
        data: &[u8],
        parquet_type: ParquetType,
        num_values: usize,
    ) -> Result<Vec<Value>, ParquetError> {
        let mut values = Vec::with_capacity(num_values);
        let mut offset = 0;

        for _ in 0..num_values {
            if offset >= data.len() {
                values.push(Value::Null);
                continue;
            }

            let value = match parquet_type {
                ParquetType::Boolean => {
                    let byte_idx = offset / 8;
                    let bit_idx = offset % 8;
                    if byte_idx < data.len() {
                        let bit = (data[byte_idx] >> bit_idx) & 1;
                        offset += 1;
                        Value::Boolean(bit != 0)
                    } else {
                        Value::Null
                    }
                }
                ParquetType::Int32 => {
                    if offset + 4 <= data.len() {
                        let val = i32::from_le_bytes([
                            data[offset],
                            data[offset + 1],
                            data[offset + 2],
                            data[offset + 3],
                        ]);
                        offset += 4;
                        Value::Integer(val as i64)
                    } else {
                        Value::Null
                    }
                }
                ParquetType::Int64 => {
                    if offset + 8 <= data.len() {
                        let val = i64::from_le_bytes([
                            data[offset],
                            data[offset + 1],
                            data[offset + 2],
                            data[offset + 3],
                            data[offset + 4],
                            data[offset + 5],
                            data[offset + 6],
                            data[offset + 7],
                        ]);
                        offset += 8;
                        Value::Integer(val)
                    } else {
                        Value::Null
                    }
                }
                ParquetType::Float => {
                    if offset + 4 <= data.len() {
                        let val = f32::from_le_bytes([
                            data[offset],
                            data[offset + 1],
                            data[offset + 2],
                            data[offset + 3],
                        ]);
                        offset += 4;
                        Value::Float(val as f64)
                    } else {
                        Value::Null
                    }
                }
                ParquetType::Double => {
                    if offset + 8 <= data.len() {
                        let val = f64::from_le_bytes([
                            data[offset],
                            data[offset + 1],
                            data[offset + 2],
                            data[offset + 3],
                            data[offset + 4],
                            data[offset + 5],
                            data[offset + 6],
                            data[offset + 7],
                        ]);
                        offset += 8;
                        Value::Float(val)
                    } else {
                        Value::Null
                    }
                }
                ParquetType::ByteArray => {
                    if offset + 4 <= data.len() {
                        let len = u32::from_le_bytes([
                            data[offset],
                            data[offset + 1],
                            data[offset + 2],
                            data[offset + 3],
                        ]) as usize;
                        offset += 4;
                        if offset + len <= data.len() {
                            let s = String::from_utf8_lossy(&data[offset..offset + len]).to_string();
                            offset += len;
                            Value::String(s)
                        } else {
                            Value::Null
                        }
                    } else {
                        Value::Null
                    }
                }
                _ => Value::Null,
            };

            values.push(value);
        }

        Ok(values)
    }

    fn decode_dictionary_values(
        &self,
        data: &[u8],
        dictionary: &[Value],
        num_values: usize,
    ) -> Result<Vec<Value>, ParquetError> {
        // RLE/Bit-packed hybrid encoding for dictionary indices
        if data.is_empty() {
            return Ok(vec![Value::Null; num_values]);
        }

        let bit_width = data[0] as usize;
        let mut values = Vec::with_capacity(num_values);
        let mut offset = 1;

        while values.len() < num_values && offset < data.len() {
            let header = self.read_varint(&data[offset..])?;
            offset += self.varint_size(header);

            if header & 1 == 1 {
                // Bit-packed run
                let count = ((header >> 1) * 8) as usize;
                let bytes_needed = (count * bit_width + 7) / 8;

                if offset + bytes_needed <= data.len() {
                    for i in 0..count.min(num_values - values.len()) {
                        let bit_offset = i * bit_width;
                        let byte_offset = bit_offset / 8;
                        let bit_shift = bit_offset % 8;

                        let mut idx = 0usize;
                        let mut bits_remaining = bit_width;
                        let mut current_shift = 0;

                        while bits_remaining > 0 {
                            let byte_idx = offset + byte_offset + (bit_shift + current_shift) / 8;
                            if byte_idx >= data.len() {
                                break;
                            }
                            let bits_in_byte = 8 - ((bit_shift + current_shift) % 8);
                            let bits_to_read = bits_remaining.min(bits_in_byte);
                            let mask = (1 << bits_to_read) - 1;
                            let shift = (bit_shift + current_shift) % 8;
                            idx |= (((data[byte_idx] >> shift) & mask as u8) as usize) << current_shift;
                            bits_remaining -= bits_to_read;
                            current_shift += bits_to_read;
                        }

                        let value = dictionary.get(idx).cloned().unwrap_or(Value::Null);
                        values.push(value);
                    }
                    offset += bytes_needed;
                }
            } else {
                // RLE run
                let count = (header >> 1) as usize;
                let bytes_needed = (bit_width + 7) / 8;

                if offset + bytes_needed <= data.len() {
                    let mut idx = 0usize;
                    for i in 0..bytes_needed {
                        idx |= (data[offset + i] as usize) << (i * 8);
                    }
                    idx &= (1 << bit_width) - 1;

                    let value = dictionary.get(idx).cloned().unwrap_or(Value::Null);
                    for _ in 0..count.min(num_values - values.len()) {
                        values.push(value.clone());
                    }
                    offset += bytes_needed;
                }
            }
        }

        // Pad with nulls if needed
        while values.len() < num_values {
            values.push(Value::Null);
        }

        Ok(values)
    }

    fn read_varint(&self, data: &[u8]) -> Result<u32, ParquetError> {
        let mut result = 0u32;
        let mut shift = 0;

        for byte in data {
            result |= ((byte & 0x7F) as u32) << shift;
            if byte & 0x80 == 0 {
                return Ok(result);
            }
            shift += 7;
            if shift >= 32 {
                return Err(ParquetError::InvalidFormat("Varint too long".into()));
            }
        }

        Err(ParquetError::InvalidFormat("Incomplete varint".into()))
    }

    fn varint_size(&self, value: u32) -> usize {
        if value < 128 {
            1
        } else if value < 16384 {
            2
        } else if value < 2097152 {
            3
        } else if value < 268435456 {
            4
        } else {
            5
        }
    }
}

#[derive(Debug)]
struct PageHeader {
    page_type: PageType,
    uncompressed_page_size: i32,
    compressed_page_size: i32,
    num_values: i32,
}

// Simple Thrift decoder for compact protocol
struct ThriftDecoder<'a> {
    data: &'a [u8],
    pos: usize,
    last_field_id: i16,
}

impl<'a> ThriftDecoder<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            pos: 0,
            last_field_id: 0,
        }
    }

    fn position(&self) -> usize {
        self.pos
    }

    fn read_field_header(&mut self) -> Result<Option<(i16, u8)>, ParquetError> {
        if self.pos >= self.data.len() {
            return Ok(None);
        }

        let byte = self.data[self.pos];
        self.pos += 1;

        if byte == 0 {
            // Stop field
            return Ok(None);
        }

        let field_type = byte & 0x0F;
        let delta = (byte >> 4) as i16;

        let field_id = if delta == 0 {
            // Full field id follows
            self.read_i16()?
        } else {
            self.last_field_id + delta
        };

        self.last_field_id = field_id;
        Ok(Some((field_id, field_type)))
    }

    fn read_i16(&mut self) -> Result<i16, ParquetError> {
        let val = self.read_varint()? as i16;
        Ok((val >> 1) ^ -(val & 1))
    }

    fn read_i32(&mut self) -> Result<i32, ParquetError> {
        let val = self.read_varint()? as i32;
        Ok((val >> 1) ^ -(val & 1))
    }

    fn read_i64(&mut self) -> Result<i64, ParquetError> {
        let val = self.read_varint_64()?;
        Ok(((val >> 1) as i64) ^ -((val & 1) as i64))
    }

    fn read_string(&mut self) -> Result<String, ParquetError> {
        let len = self.read_varint()? as usize;
        if self.pos + len > self.data.len() {
            return Err(ParquetError::InvalidFormat("String length exceeds data".into()));
        }
        let s = String::from_utf8_lossy(&self.data[self.pos..self.pos + len]).to_string();
        self.pos += len;
        Ok(s)
    }

    fn read_list_header(&mut self) -> Result<ListHeader, ParquetError> {
        if self.pos >= self.data.len() {
            return Err(ParquetError::InvalidFormat("Unexpected end of data".into()));
        }

        let byte = self.data[self.pos];
        self.pos += 1;

        let size_and_type = byte;
        let elem_type = size_and_type & 0x0F;
        let size = if (size_and_type >> 4) == 0x0F {
            self.read_varint()? as i32
        } else {
            (size_and_type >> 4) as i32
        };

        Ok(ListHeader { size, elem_type })
    }

    fn read_varint(&mut self) -> Result<u32, ParquetError> {
        let mut result = 0u32;
        let mut shift = 0;

        while self.pos < self.data.len() {
            let byte = self.data[self.pos];
            self.pos += 1;

            result |= ((byte & 0x7F) as u32) << shift;
            if byte & 0x80 == 0 {
                return Ok(result);
            }
            shift += 7;
            if shift >= 32 {
                return Err(ParquetError::InvalidFormat("Varint too long".into()));
            }
        }

        Err(ParquetError::InvalidFormat("Incomplete varint".into()))
    }

    fn read_varint_64(&mut self) -> Result<u64, ParquetError> {
        let mut result = 0u64;
        let mut shift = 0;

        while self.pos < self.data.len() {
            let byte = self.data[self.pos];
            self.pos += 1;

            result |= ((byte & 0x7F) as u64) << shift;
            if byte & 0x80 == 0 {
                return Ok(result);
            }
            shift += 7;
            if shift >= 64 {
                return Err(ParquetError::InvalidFormat("Varint too long".into()));
            }
        }

        Err(ParquetError::InvalidFormat("Incomplete varint".into()))
    }

    fn skip_field(&mut self, field_type: u8) -> Result<(), ParquetError> {
        match field_type {
            1 | 2 => {} // bool true/false - no additional data
            3 => { self.read_varint()?; } // i8
            4 => { self.read_varint()?; } // i16
            5 => { self.read_varint()?; } // i32
            6 => { self.read_varint_64()?; } // i64
            7 => { self.pos += 8; } // double
            8 => { // binary/string
                let len = self.read_varint()? as usize;
                self.pos += len;
            }
            9 => { // list
                let header = self.read_list_header()?;
                for _ in 0..header.size {
                    self.skip_field(header.elem_type)?;
                }
            }
            11 => { // map
                let header = self.read_list_header()?;
                let key_type = (header.elem_type >> 4) & 0x0F;
                let val_type = header.elem_type & 0x0F;
                for _ in 0..header.size {
                    self.skip_field(key_type)?;
                    self.skip_field(val_type)?;
                }
            }
            12 => { // struct
                while let Some((_, inner_type)) = self.read_field_header()? {
                    self.skip_field(inner_type)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
}

struct ListHeader {
    size: i32,
    elem_type: u8,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parquet_reader_creation() {
        let reader = ParquetReader::new();
        // Just verify it can be created
        let _ = reader;
    }
}
