// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.30.0
// 	protoc        (unknown)
// source: fri/v1/fri.proto

package v1

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Events struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Timestamp []uint64 `protobuf:"varint,1,rep,packed,name=timestamp,proto3" json:"timestamp,omitempty"`
	Name      []uint64 `protobuf:"varint,2,rep,packed,name=name,proto3" json:"name,omitempty"`
	Attr      []*Attr  `protobuf:"bytes,3,rep,name=attr,proto3" json:"attr,omitempty"`
}

func (x *Events) Reset() {
	*x = Events{}
	if protoimpl.UnsafeEnabled {
		mi := &file_fri_v1_fri_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Events) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Events) ProtoMessage() {}

func (x *Events) ProtoReflect() protoreflect.Message {
	mi := &file_fri_v1_fri_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Events.ProtoReflect.Descriptor instead.
func (*Events) Descriptor() ([]byte, []int) {
	return file_fri_v1_fri_proto_rawDescGZIP(), []int{0}
}

func (x *Events) GetTimestamp() []uint64 {
	if x != nil {
		return x.Timestamp
	}
	return nil
}

func (x *Events) GetName() []uint64 {
	if x != nil {
		return x.Name
	}
	return nil
}

func (x *Events) GetAttr() []*Attr {
	if x != nil {
		return x.Attr
	}
	return nil
}

type Links struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TraceId    []uint64 `protobuf:"varint,1,rep,packed,name=trace_id,json=traceId,proto3" json:"trace_id,omitempty"`
	SpanId     []uint64 `protobuf:"varint,2,rep,packed,name=span_id,json=spanId,proto3" json:"span_id,omitempty"`
	Attr       []*Attr  `protobuf:"bytes,3,rep,name=attr,proto3" json:"attr,omitempty"`
	TraceState []uint64 `protobuf:"varint,4,rep,packed,name=trace_state,json=traceState,proto3" json:"trace_state,omitempty"`
}

func (x *Links) Reset() {
	*x = Links{}
	if protoimpl.UnsafeEnabled {
		mi := &file_fri_v1_fri_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Links) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Links) ProtoMessage() {}

func (x *Links) ProtoReflect() protoreflect.Message {
	mi := &file_fri_v1_fri_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Links.ProtoReflect.Descriptor instead.
func (*Links) Descriptor() ([]byte, []int) {
	return file_fri_v1_fri_proto_rawDescGZIP(), []int{1}
}

func (x *Links) GetTraceId() []uint64 {
	if x != nil {
		return x.TraceId
	}
	return nil
}

func (x *Links) GetSpanId() []uint64 {
	if x != nil {
		return x.SpanId
	}
	return nil
}

func (x *Links) GetAttr() []*Attr {
	if x != nil {
		return x.Attr
	}
	return nil
}

func (x *Links) GetTraceState() []uint64 {
	if x != nil {
		return x.TraceState
	}
	return nil
}

type Attr struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Value []uint64 `protobuf:"varint,1,rep,packed,name=value,proto3" json:"value,omitempty"`
}

func (x *Attr) Reset() {
	*x = Attr{}
	if protoimpl.UnsafeEnabled {
		mi := &file_fri_v1_fri_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Attr) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Attr) ProtoMessage() {}

func (x *Attr) ProtoReflect() protoreflect.Message {
	mi := &file_fri_v1_fri_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Attr.ProtoReflect.Descriptor instead.
func (*Attr) Descriptor() ([]byte, []int) {
	return file_fri_v1_fri_proto_rawDescGZIP(), []int{2}
}

func (x *Attr) GetValue() []uint64 {
	if x != nil {
		return x.Value
	}
	return nil
}

type BitDepth struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Tracks last observed bitdepth for ingested values. Not all fields are
	// tracked so, only BSI fields use this.
	//
	// Upper bounnd is 64.
	BitDepth map[uint64]uint64 `protobuf:"bytes,1,rep,name=bit_depth,json=bitDepth,proto3" json:"bit_depth,omitempty" protobuf_key:"varint,1,opt,name=key,proto3" protobuf_val:"varint,2,opt,name=value,proto3"`
}

func (x *BitDepth) Reset() {
	*x = BitDepth{}
	if protoimpl.UnsafeEnabled {
		mi := &file_fri_v1_fri_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BitDepth) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BitDepth) ProtoMessage() {}

func (x *BitDepth) ProtoReflect() protoreflect.Message {
	mi := &file_fri_v1_fri_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BitDepth.ProtoReflect.Descriptor instead.
func (*BitDepth) Descriptor() ([]byte, []int) {
	return file_fri_v1_fri_proto_rawDescGZIP(), []int{3}
}

func (x *BitDepth) GetBitDepth() map[uint64]uint64 {
	if x != nil {
		return x.BitDepth
	}
	return nil
}

var File_fri_v1_fri_proto protoreflect.FileDescriptor

var file_fri_v1_fri_proto_rawDesc = []byte{
	0x0a, 0x10, 0x66, 0x72, 0x69, 0x2f, 0x76, 0x31, 0x2f, 0x66, 0x72, 0x69, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x12, 0x02, 0x76, 0x31, 0x22, 0x58, 0x0a, 0x06, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x73,
	0x12, 0x1c, 0x0a, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x01, 0x20,
	0x03, 0x28, 0x04, 0x52, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x12, 0x12,
	0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x02, 0x20, 0x03, 0x28, 0x04, 0x52, 0x04, 0x6e, 0x61,
	0x6d, 0x65, 0x12, 0x1c, 0x0a, 0x04, 0x61, 0x74, 0x74, 0x72, 0x18, 0x03, 0x20, 0x03, 0x28, 0x0b,
	0x32, 0x08, 0x2e, 0x76, 0x31, 0x2e, 0x41, 0x74, 0x74, 0x72, 0x52, 0x04, 0x61, 0x74, 0x74, 0x72,
	0x22, 0x7a, 0x0a, 0x05, 0x4c, 0x69, 0x6e, 0x6b, 0x73, 0x12, 0x19, 0x0a, 0x08, 0x74, 0x72, 0x61,
	0x63, 0x65, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x03, 0x28, 0x04, 0x52, 0x07, 0x74, 0x72, 0x61,
	0x63, 0x65, 0x49, 0x64, 0x12, 0x17, 0x0a, 0x07, 0x73, 0x70, 0x61, 0x6e, 0x5f, 0x69, 0x64, 0x18,
	0x02, 0x20, 0x03, 0x28, 0x04, 0x52, 0x06, 0x73, 0x70, 0x61, 0x6e, 0x49, 0x64, 0x12, 0x1c, 0x0a,
	0x04, 0x61, 0x74, 0x74, 0x72, 0x18, 0x03, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x08, 0x2e, 0x76, 0x31,
	0x2e, 0x41, 0x74, 0x74, 0x72, 0x52, 0x04, 0x61, 0x74, 0x74, 0x72, 0x12, 0x1f, 0x0a, 0x0b, 0x74,
	0x72, 0x61, 0x63, 0x65, 0x5f, 0x73, 0x74, 0x61, 0x74, 0x65, 0x18, 0x04, 0x20, 0x03, 0x28, 0x04,
	0x52, 0x0a, 0x74, 0x72, 0x61, 0x63, 0x65, 0x53, 0x74, 0x61, 0x74, 0x65, 0x22, 0x1c, 0x0a, 0x04,
	0x41, 0x74, 0x74, 0x72, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x01, 0x20,
	0x03, 0x28, 0x04, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x80, 0x01, 0x0a, 0x08, 0x42,
	0x69, 0x74, 0x44, 0x65, 0x70, 0x74, 0x68, 0x12, 0x37, 0x0a, 0x09, 0x62, 0x69, 0x74, 0x5f, 0x64,
	0x65, 0x70, 0x74, 0x68, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x76, 0x31, 0x2e,
	0x42, 0x69, 0x74, 0x44, 0x65, 0x70, 0x74, 0x68, 0x2e, 0x42, 0x69, 0x74, 0x44, 0x65, 0x70, 0x74,
	0x68, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x08, 0x62, 0x69, 0x74, 0x44, 0x65, 0x70, 0x74, 0x68,
	0x1a, 0x3b, 0x0a, 0x0d, 0x42, 0x69, 0x74, 0x44, 0x65, 0x70, 0x74, 0x68, 0x45, 0x6e, 0x74, 0x72,
	0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x04, 0x52, 0x03,
	0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x04, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01, 0x42, 0x64, 0x0a,
	0x06, 0x63, 0x6f, 0x6d, 0x2e, 0x76, 0x31, 0x42, 0x08, 0x46, 0x72, 0x69, 0x50, 0x72, 0x6f, 0x74,
	0x6f, 0x50, 0x01, 0x5a, 0x28, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f,
	0x67, 0x65, 0x72, 0x6e, 0x65, 0x73, 0x74, 0x2f, 0x66, 0x72, 0x69, 0x65, 0x72, 0x65, 0x6e, 0x2f,
	0x67, 0x65, 0x6e, 0x2f, 0x67, 0x6f, 0x2f, 0x66, 0x72, 0x69, 0x2f, 0x76, 0x31, 0xa2, 0x02, 0x03,
	0x56, 0x58, 0x58, 0xaa, 0x02, 0x02, 0x56, 0x31, 0xca, 0x02, 0x02, 0x56, 0x31, 0xe2, 0x02, 0x0e,
	0x56, 0x31, 0x5c, 0x47, 0x50, 0x42, 0x4d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0xea, 0x02,
	0x02, 0x56, 0x31, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_fri_v1_fri_proto_rawDescOnce sync.Once
	file_fri_v1_fri_proto_rawDescData = file_fri_v1_fri_proto_rawDesc
)

func file_fri_v1_fri_proto_rawDescGZIP() []byte {
	file_fri_v1_fri_proto_rawDescOnce.Do(func() {
		file_fri_v1_fri_proto_rawDescData = protoimpl.X.CompressGZIP(file_fri_v1_fri_proto_rawDescData)
	})
	return file_fri_v1_fri_proto_rawDescData
}

var file_fri_v1_fri_proto_msgTypes = make([]protoimpl.MessageInfo, 5)
var file_fri_v1_fri_proto_goTypes = []interface{}{
	(*Events)(nil),   // 0: v1.Events
	(*Links)(nil),    // 1: v1.Links
	(*Attr)(nil),     // 2: v1.Attr
	(*BitDepth)(nil), // 3: v1.BitDepth
	nil,              // 4: v1.BitDepth.BitDepthEntry
}
var file_fri_v1_fri_proto_depIdxs = []int32{
	2, // 0: v1.Events.attr:type_name -> v1.Attr
	2, // 1: v1.Links.attr:type_name -> v1.Attr
	4, // 2: v1.BitDepth.bit_depth:type_name -> v1.BitDepth.BitDepthEntry
	3, // [3:3] is the sub-list for method output_type
	3, // [3:3] is the sub-list for method input_type
	3, // [3:3] is the sub-list for extension type_name
	3, // [3:3] is the sub-list for extension extendee
	0, // [0:3] is the sub-list for field type_name
}

func init() { file_fri_v1_fri_proto_init() }
func file_fri_v1_fri_proto_init() {
	if File_fri_v1_fri_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_fri_v1_fri_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Events); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_fri_v1_fri_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Links); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_fri_v1_fri_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Attr); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_fri_v1_fri_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BitDepth); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_fri_v1_fri_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   5,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_fri_v1_fri_proto_goTypes,
		DependencyIndexes: file_fri_v1_fri_proto_depIdxs,
		MessageInfos:      file_fri_v1_fri_proto_msgTypes,
	}.Build()
	File_fri_v1_fri_proto = out.File
	file_fri_v1_fri_proto_rawDesc = nil
	file_fri_v1_fri_proto_goTypes = nil
	file_fri_v1_fri_proto_depIdxs = nil
}
