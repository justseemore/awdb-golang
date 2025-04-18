package awdb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
)

// AWDB文件特征值
// var awDataMarker = []byte("\\u57c3\\u6587\\u79d1\\u6280")

// awdbReader 保存着 awDB 文件对应的数据
// 它唯一的公共字段是 awData，其中包含来自 awDB 文件的元数据
type awdbReader struct {
	hasMappedFile bool
	buffer        []byte
	awData        awMetaData
	startLen      uint
	baseOffset    uint
}

// awData 元数据保存 awDB 文件解码的元数据。
// 它具有支持的 IP 版本等信息
type awMetaData struct {
	Node_Count  uint          `awdb:"node_count"`
	IP_Version  string        `awdb:"ip_version"`
	Byte_Len    uint          `awdb:"byte_len"`
	Languages   string        `awdb:"languages"`
	File_Name   string        `awdb:"file_name"`
	Create_Time string        `awdb:"create_time"`
	Company_ID  string        `awdb:"company_id"`
	Version     string        `awdb:"version"`
	Decode_Type uint          `awdb:"decode_type"`
	Columns     []interface{} `awdb:"columns"`
}

const (
	_Arrary intTypeData = iota + 1
	_Pointer
	_String
	_LongString
	_Uint
	_Int
	_Float
	_Double
)

type intTypeData int

// Openfile 打开文件，加载到内存，返回一个reader对象
func Openfile(file string) (*awdbReader, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}

	fileSize := int(stat.Size())
	mapview, err := mmap(int(f.Fd()), fileSize)
	if err != nil {
		return nil, err
	}
	reader, err := ReadBytes(mapview)
	if err != nil {
		_ = munmap(mapview)
		return nil, err
	}
	reader.hasMappedFile = true
	return reader, nil
}

// ReadBytes 获取对应于 awDB 文件的字节切片并返回 Reader对象或错误。
func ReadBytes(buffer []byte) (*awdbReader, error) {
	// 获取metadata长度值字节切片
	metaDataLenByte := buffer[:2]
	metaDataLen := bytesToIntU(metaDataLenByte)
	// 数据开始
	dataSectionStart := 2 + metaDataLen
	// +++++++++++++++++++++++++++++++++
	// 获取metadata内容，根据metaDataLen长度，解析metadata内容
	// +++++++++++++++++++++++++++++++++
	matadatabyte := buffer[2:dataSectionStart]
	var metadata awMetaData
	err := json.Unmarshal(matadatabyte, &metadata)
	if err != nil {
		return nil, err
	}
	// 根据metadata内容，得到node_count值
	NodeCount := metadata.Node_Count
	// fmt.Println(metadata.Decode_Type)
	base_offset := NodeCount*metadata.Byte_Len*2 + dataSectionStart

	reader := &awdbReader{
		buffer:     buffer,
		awData:     metadata,
		startLen:   dataSectionStart,
		baseOffset: base_offset,
	}
	return reader, err
}

// SearchIP 获取IP索引，解析信息
func (r *awdbReader) SearchIP(ipaddress string) (error, map[string]interface{}) {
	if r.buffer == nil {
		return errors.New("cannot call Lookup on a closed database"), nil
	}
	ip := handleIP(ipaddress)
	// 获取IP索引
	pointer, err := r.searchIPPointer(ip)
	if pointer == 0 || err != nil {
		return err, nil
	}
	// 根据IP索引解析信息
	return r.getData(pointer)
}

// handleIP 处理输入文本，转换成net.IP类型
func handleIP(ipaddress string) net.IP {
	var ip net.IP
	ipInt, err := strconv.Atoi(ipaddress)
	if err == nil {
		if ipInt > 4294967296 {
			return nil
		}
		// 整数是IPv4地址
		ip = make(net.IP, net.IPv4len)
		for i := net.IPv4len - 1; i >= 0; i-- {
			ip[i] = byte(ipInt & 0xff)
			ipInt >>= 8
		}
		ip = ip.To16()
		return ip
	} else {
		ip = net.ParseIP(ipaddress)
		return ip
	}
}

// 解析索引，获取信息，返回到result
func (r *awdbReader) getData(pointer uint) (error, map[string]interface{}) {
	offset, err := r.getDataPointer(pointer)
	if err != nil {
		return err, nil
	}
	// 根据不同Decode_Type进行不同的数据解析方式处理，后期可拓展
	switch r.awData.Decode_Type {
	case 1:
		resDone, _, errs := r.readDecode(offset)
		if errs != nil {
			return errs, nil
		}
		keys := r.awData.Columns
		values := resDone.([]interface{})
		res := MapKeyValue(keys, values)
		return nil, res
	case 2:
		dataLen := bytesToIntU(r.buffer[offset : offset+4])
		offset += 4
		tempList := strings.Split(string(r.buffer[offset:offset+dataLen]), "\t")
		keyLen := len(r.awData.Columns)
		res := make(map[string]interface{}, keyLen)
		for i, value := range r.awData.Columns {
			res[value.(string)] = tempList[i]
		}
		return nil, res
	default:
		return newfailDatabaseError("unknown decodetype: %d", r.awData.Decode_Type), nil
	}
}

// 根据索引值获取数据offset
func (r *awdbReader) getDataPointer(pointer uint) (uint, error) {
	resolved := r.baseOffset + pointer - r.awData.Node_Count - 10
	if resolved >= uint(len(r.buffer)) {
		return 0, newfailDatabaseError("the aw DB file's search tree is corrupt")
	}
	return resolved, nil
}

// 根据offset处理不同类型数据
func (r *awdbReader) readDecode(offset uint) (interface{}, uint, error) {
	dataType := intTypeData(r.buffer[offset])
	newOffset := offset + 1
	switch dataType {
	case _Arrary:
		return r.awDecodeArrary(newOffset)
	case _Pointer:
		return r.awDecodePointer(newOffset)
	case _String:
		return r.awDecodeString(newOffset)
	case _LongString:
		return r.awDecodeLString(newOffset)
	case _Uint:
		return r.awDecodeUint(newOffset)
	case _Int:
		return r.awDecodeInt(newOffset)
	case _Float:
		return r.awDecodeFloat(newOffset)
	case _Double:
		return r.awDecodeDouble(newOffset)
	default:
		return nil, 0, newfailDatabaseError("unknown type: %d", dataType)
	}
}

// 处理数组数据类型
func (r *awdbReader) awDecodeArrary(newOffset uint) ([]interface{}, uint, error) {
	dataLen := uint(r.buffer[newOffset])
	newOffset += 1
	var array []interface{}
	i := uint(0)
	for ; i < dataLen; i++ {
		value, Offset, _ := r.readDecode(newOffset)
		newOffset = Offset
		array = append(array, value)
	}
	return array, newOffset, nil
}

// 处理指针数据类型
func (r *awdbReader) awDecodePointer(offset uint) (interface{}, uint, error) {
	dataLen := uint(r.buffer[offset])
	offset += 1
	newOffset := offset + dataLen
	buf := bytesToIntU(r.buffer[offset:newOffset])
	pointer := r.baseOffset + buf
	value, _, _ := r.readDecode(pointer)
	return value, newOffset, nil
}

// 处理string数据类型
func (r *awdbReader) awDecodeString(newoffset uint) (string, uint, error) {
	dataLen := uint(r.buffer[newoffset])
	newoffset += 1
	tempOffset := newoffset + dataLen
	return string(r.buffer[newoffset:tempOffset]), tempOffset, nil
}

// 处理长string数据类型
func (r *awdbReader) awDecodeLString(newoffset uint) (string, uint, error) {
	size := uint(r.buffer[newoffset])
	newoffset += 1
	dataLen := bytesToIntU(r.buffer[newoffset : newoffset+size])
	newoffset += size
	tempOffset := newoffset + dataLen

	return string(r.buffer[newoffset:tempOffset]), tempOffset, nil
}

// 处理无符号Int类型
func (r *awdbReader) awDecodeUint(newoffset uint) (uint, uint, error) {
	dataLen := uint(r.buffer[newoffset])
	newoffset += 1
	tempOffset := newoffset + dataLen
	uintValue := bytesToIntU(r.buffer[newoffset:tempOffset])
	return uintValue, tempOffset, nil
}

// 处理有符号Int类型
func (r *awdbReader) awDecodeInt(newoffset uint) (int, uint, error) {
	tempOffset := newoffset + 4
	intValue := bytesToIntS(r.buffer[newoffset:tempOffset])
	return intValue, tempOffset, nil
}

// 处理float数据类型
func (r *awdbReader) awDecodeFloat(newoffset uint) (float32, uint, error) {
	tempOffset := newoffset + 4
	floatValue := ByteToFloat32(r.buffer[newoffset:tempOffset])
	return floatValue, tempOffset, nil
}

// 处理double数据类型
func (r *awdbReader) awDecodeDouble(newoffset uint) (float64, uint, error) {
	tempOffset := newoffset + 8
	doubleValue := ByteToFloat64(r.buffer[newoffset:tempOffset])
	return doubleValue, tempOffset, nil
}

// 根据IP获取索引
func (r *awdbReader) searchIPPointer(ip net.IP) (uint, error) {
	if ip == nil {
		return 0, errors.New("please check the input IP format")
	}
	if r.awData.IP_Version != "4_6" {
		// 16位IPv4转4位，4_6版本中无需转换
		ipV4Address := ip.To4()
		if ipV4Address != nil {
			ip = ipV4Address
		}
	}
	if len(ip) == 16 && r.awData.IP_Version == "4" {
		return 0, fmt.Errorf("error search '%s': you attempted to search an IPv6 address in an IPv4-only database", ip.String())
	}
	if len(ip) == 4 && r.awData.IP_Version == "6" {
		return 0, fmt.Errorf("error search '%s': you attempted to search an IPv4 address in an IPv6-only database", ip.String())
	}
	// 查找树的叶节点
	bitCount := uint(len(ip) * 8)
	nodeIndex, err := r.searchTree(ip, bitCount)
	if err != nil {
		return 0, err
	}
	return nodeIndex, nil
}

// 查找树的叶节点
func (r *awdbReader) searchTree(ip net.IP, bitCount uint) (uint, error) {
	nodeCount := r.awData.Node_Count
	bytelen := r.awData.Byte_Len
	startLen := r.startLen
	var nodeIndex = uint(0)
	i := uint(0)
	for ; i < bitCount && nodeIndex < nodeCount; i++ {
		bit := uint(1) & (uint(ip[i>>3]) >> (7 - (i % 8)))
		offset := nodeIndex*bytelen*2 + bit*bytelen + startLen
		nodeByte := r.buffer[offset : offset+bytelen]
		nodeIndex = bytesToIntU(nodeByte)
	}
	if nodeIndex == nodeCount {
		fmt.Println("invalid nodeIndex in search tree")
		return 0, nil
	}
	if nodeIndex > nodeCount {
		return nodeIndex, nil
	}
	return 0, errors.New("invalid nodeIndex in search tree")
}

// MapKeyValue 映射数据键与值，生成最终结果map
func MapKeyValue(keys []interface{}, values []interface{}) map[string]interface{} {
	valuesLen := len(values)
	keyLen := len(keys)
	if keyLen == valuesLen {
		resultDict := make(map[string]interface{}, keyLen)
		for i, k := range keys {
			key, ok := k.(string)
			if !ok {
				errMsg := fmt.Sprintf("Key at index %d is not a string", i)
				panic(errMsg)
			}
			resultDict[key] = values[i]
		}
		return resultDict
	} else {
		count := valuesLen - 1
		forepartDict := make(map[string]interface{}, count)
		for i := 0; i < count; i++ {
			key, ok := keys[i].(string)
			if !ok {
				errMsg := fmt.Sprintf("Key at index %d is not a string", i)
				panic(errMsg)
			}
			forepartDict[key] = values[i]
		}
		keysList := keys[keyLen-1].([]interface{})
		valuesList := values[valuesLen-1].([]interface{})
		multiAreasName := keys[keyLen-2].(string)

		endDict := make(map[string]interface{})
		var multiAreas []interface{}
		for _, row := range valuesList {
			rowMap := make(map[string]interface{})
			for i, key := range keysList {
				rowMap[key.(string)] = row.([]interface{})[i]
			}
			multiAreas = append(multiAreas, rowMap)
		}
		endDict[multiAreasName] = multiAreas
		resultDict := make(map[string]interface{})
		for k, v := range forepartDict {
			resultDict[k] = v
		}
		for k, v := range endDict {
			resultDict[k] = v
		}
		return resultDict
	}
}

// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// 其他函数
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

// Closefile 从虚拟内存中取消映射awdb文件并将资源返回给系统
func (r *awdbReader) Closefile() {
	if r.hasMappedFile {
		runtime.SetFinalizer(r, nil)
		r.hasMappedFile = false
		// 释放资源
		err := munmap(r.buffer)
		if err != nil {
			return
		}
	}
	r.buffer = nil
}

// ByteToFloat32  字节数组转换成浮点型
func ByteToFloat32(bytes []byte) float32 {
	bits := binary.BigEndian.Uint32(bytes)
	return math.Float32frombits(bits)
}

// ByteToFloat64 字节数组转换成浮点型
func ByteToFloat64(bytes []byte) float64 {
	bits := binary.BigEndian.Uint64(bytes)
	return math.Float64frombits(bits)
}

// 字节数(大端)组转成int(无符号的)
func bytesToIntU(b []byte) uint {
	byteLen := len(b)
	if byteLen == 3 {
		b = append([]byte{0}, b...)
	}
	if byteLen == 5 {
		b = append([]byte{0, 0, 0}, b...)
	}
	switch len(b) {
	case 0:
		return uint(0)
	case 1:
		return uint(b[0])
	case 2:
		var tmp uint16
		tmp = binary.BigEndian.Uint16(b)
		return uint(tmp)
	case 4:
		var tmp uint32
		tmp = binary.BigEndian.Uint32(b)
		return uint(tmp)
	default:
		var tmp uint64
		tmp = binary.BigEndian.Uint64(b)
		return uint(tmp)
	}
}

// 字节数(大端)组转成int(有符号)
func bytesToIntS(b []byte) int {
	if len(b) == 3 {
		b = append([]byte{0}, b...)
	}
	if len(b) == 5 {
		b = append([]byte{0, 0, 0}, b...)
	}
	bytesBuffer := bytes.NewBuffer(b)
	switch len(b) {
	case 1:
		var tmp int8
		binary.Read(bytesBuffer, binary.BigEndian, &tmp)
		return int(tmp)
	case 2:
		var tmp int16
		binary.Read(bytesBuffer, binary.BigEndian, &tmp)
		return int(tmp)
	case 4:
		var tmp int32
		binary.Read(bytesBuffer, binary.BigEndian, &tmp)
		return int(tmp)
	default:
		var tmp int64
		binary.Read(bytesBuffer, binary.BigEndian, &tmp)
		return int(tmp)
	}
}

// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// error
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

// failDatabaseError 当数据包含无效数据且无法解析时，返回 failDatabaseError。
type failDatabaseError struct {
	message string
}

func newfailDatabaseError(format string, args ...interface{}) failDatabaseError {
	return failDatabaseError{fmt.Sprintf(format, args...)}
}

func (e failDatabaseError) Error() string {
	return e.message
}

// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
