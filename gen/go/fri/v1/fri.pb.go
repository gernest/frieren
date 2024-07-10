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

type Sample_Kind int32

const (
	Sample_NONE      Sample_Kind = 0
	Sample_FLOAT     Sample_Kind = 1
	Sample_HISTOGRAM Sample_Kind = 2
)

// Enum value maps for Sample_Kind.
var (
	Sample_Kind_name = map[int32]string{
		0: "NONE",
		1: "FLOAT",
		2: "HISTOGRAM",
	}
	Sample_Kind_value = map[string]int32{
		"NONE":      0,
		"FLOAT":     1,
		"HISTOGRAM": 2,
	}
)

func (x Sample_Kind) Enum() *Sample_Kind {
	p := new(Sample_Kind)
	*p = x
	return p
}

func (x Sample_Kind) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (Sample_Kind) Descriptor() protoreflect.EnumDescriptor {
	return file_fri_v1_fri_proto_enumTypes[0].Descriptor()
}

func (Sample_Kind) Type() protoreflect.EnumType {
	return &file_fri_v1_fri_proto_enumTypes[0]
}

func (x Sample_Kind) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use Sample_Kind.Descriptor instead.
func (Sample_Kind) EnumDescriptor() ([]byte, []int) {
	return file_fri_v1_fri_proto_rawDescGZIP(), []int{3, 0}
}

type FieldViewInfo struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Shards map[uint64]*Shard `protobuf:"bytes,1,rep,name=shards,proto3" json:"shards,omitempty" protobuf_key:"varint,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

func (x *FieldViewInfo) Reset() {
	*x = FieldViewInfo{}
	if protoimpl.UnsafeEnabled {
		mi := &file_fri_v1_fri_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *FieldViewInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FieldViewInfo) ProtoMessage() {}

func (x *FieldViewInfo) ProtoReflect() protoreflect.Message {
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

// Deprecated: Use FieldViewInfo.ProtoReflect.Descriptor instead.
func (*FieldViewInfo) Descriptor() ([]byte, []int) {
	return file_fri_v1_fri_proto_rawDescGZIP(), []int{0}
}

func (x *FieldViewInfo) GetShards() map[uint64]*Shard {
	if x != nil {
		return x.Shards
	}
	return nil
}

type Shard struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id uint64 `protobuf:"varint,1,opt,name=id,proto3" json:"id,omitempty"`
	// Tracks last observed bitdepth for ingested values. Not all fields are
	// tracked so, only BSI fields use this.
	//
	// Upper bounnd is 64.
	BitDepth map[uint64]uint64 `protobuf:"bytes,2,rep,name=bit_depth,json=bitDepth,proto3" json:"bit_depth,omitempty" protobuf_key:"varint,1,opt,name=key,proto3" protobuf_val:"varint,2,opt,name=value,proto3"`
}

func (x *Shard) Reset() {
	*x = Shard{}
	if protoimpl.UnsafeEnabled {
		mi := &file_fri_v1_fri_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Shard) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Shard) ProtoMessage() {}

func (x *Shard) ProtoReflect() protoreflect.Message {
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

// Deprecated: Use Shard.ProtoReflect.Descriptor instead.
func (*Shard) Descriptor() ([]byte, []int) {
	return file_fri_v1_fri_proto_rawDescGZIP(), []int{1}
}

func (x *Shard) GetId() uint64 {
	if x != nil {
		return x.Id
	}
	return 0
}

func (x *Shard) GetBitDepth() map[uint64]uint64 {
	if x != nil {
		return x.BitDepth
	}
	return nil
}

// Entry defines model for storing log entries.
//
// Metadata are never searched, we store them as blobs. We use repeaded field to
// take advantage of dupes, since metadata are similar to labels, they are just
// not indexed.
type Entry struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Stream    []byte   `protobuf:"bytes,1,opt,name=stream,proto3" json:"stream,omitempty"`
	Labels    []string `protobuf:"bytes,2,rep,name=labels,proto3" json:"labels,omitempty"`
	Timestamp uint64   `protobuf:"varint,3,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	Line      []byte   `protobuf:"bytes,4,opt,name=line,proto3" json:"line,omitempty"`
	Metadata  []string `protobuf:"bytes,5,rep,name=metadata,proto3" json:"metadata,omitempty"`
}

func (x *Entry) Reset() {
	*x = Entry{}
	if protoimpl.UnsafeEnabled {
		mi := &file_fri_v1_fri_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Entry) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Entry) ProtoMessage() {}

func (x *Entry) ProtoReflect() protoreflect.Message {
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

// Deprecated: Use Entry.ProtoReflect.Descriptor instead.
func (*Entry) Descriptor() ([]byte, []int) {
	return file_fri_v1_fri_proto_rawDescGZIP(), []int{2}
}

func (x *Entry) GetStream() []byte {
	if x != nil {
		return x.Stream
	}
	return nil
}

func (x *Entry) GetLabels() []string {
	if x != nil {
		return x.Labels
	}
	return nil
}

func (x *Entry) GetTimestamp() uint64 {
	if x != nil {
		return x.Timestamp
	}
	return 0
}

func (x *Entry) GetLine() []byte {
	if x != nil {
		return x.Line
	}
	return nil
}

func (x *Entry) GetMetadata() []string {
	if x != nil {
		return x.Metadata
	}
	return nil
}

// Sample models metrics sample similar to prometheus. Histogram is an integer
// histogram since we rely on prometheus trnaslation of otel.
type Sample struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Series    []byte      `protobuf:"bytes,1,opt,name=series,proto3" json:"series,omitempty"`
	Labels    []string    `protobuf:"bytes,2,rep,name=labels,proto3" json:"labels,omitempty"`
	Exemplars []byte      `protobuf:"bytes,3,opt,name=exemplars,proto3" json:"exemplars,omitempty"`
	Kind      Sample_Kind `protobuf:"varint,4,opt,name=kind,proto3,enum=v1.Sample_Kind" json:"kind,omitempty"`
	Value     float64     `protobuf:"fixed64,5,opt,name=value,proto3" json:"value,omitempty"`
	Histogram []byte      `protobuf:"bytes,6,opt,name=histogram,proto3" json:"histogram,omitempty"`
	Timestamp int64       `protobuf:"varint,7,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
}

func (x *Sample) Reset() {
	*x = Sample{}
	if protoimpl.UnsafeEnabled {
		mi := &file_fri_v1_fri_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Sample) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Sample) ProtoMessage() {}

func (x *Sample) ProtoReflect() protoreflect.Message {
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

// Deprecated: Use Sample.ProtoReflect.Descriptor instead.
func (*Sample) Descriptor() ([]byte, []int) {
	return file_fri_v1_fri_proto_rawDescGZIP(), []int{3}
}

func (x *Sample) GetSeries() []byte {
	if x != nil {
		return x.Series
	}
	return nil
}

func (x *Sample) GetLabels() []string {
	if x != nil {
		return x.Labels
	}
	return nil
}

func (x *Sample) GetExemplars() []byte {
	if x != nil {
		return x.Exemplars
	}
	return nil
}

func (x *Sample) GetKind() Sample_Kind {
	if x != nil {
		return x.Kind
	}
	return Sample_NONE
}

func (x *Sample) GetValue() float64 {
	if x != nil {
		return x.Value
	}
	return 0
}

func (x *Sample) GetHistogram() []byte {
	if x != nil {
		return x.Histogram
	}
	return nil
}

func (x *Sample) GetTimestamp() int64 {
	if x != nil {
		return x.Timestamp
	}
	return 0
}

// Span models tempo spans. When retrieving trace by ID we recostruct the
// response as a  message. To avoid excess work, we store full messages and
// extract searchable properties to fiends. This way, we can return the span as
// it was observed faster.
//
// We are agressive in indexing all indexable attributes. The goal is to have
// maximum performance and speeds, enough to allow cheaper setup/teardown in CI
// environments.
//
// There is  no cost in storing the messages because the underlying storage
// engine uses content addressable storage, data is only stored once.
type Span struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Otel resource message
	Resource           []byte   `protobuf:"bytes,1,opt,name=resource,proto3" json:"resource,omitempty"`
	ResourceAttributes []string `protobuf:"bytes,2,rep,name=resource_attributes,json=resourceAttributes,proto3" json:"resource_attributes,omitempty"`
	// Otel scope message
	Scope           []byte   `protobuf:"bytes,3,opt,name=scope,proto3" json:"scope,omitempty"`
	ScopeAttributes []string `protobuf:"bytes,4,rep,name=scope_attributes,json=scopeAttributes,proto3" json:"scope_attributes,omitempty"`
	ScopeName       string   `protobuf:"bytes,5,opt,name=scope_name,json=scopeName,proto3" json:"scope_name,omitempty"`
	ScopeVersion    string   `protobuf:"bytes,6,opt,name=scope_version,json=scopeVersion,proto3" json:"scope_version,omitempty"`
	// Otel span message
	Span              []byte   `protobuf:"bytes,7,opt,name=span,proto3" json:"span,omitempty"`
	SpanAttributes    []string `protobuf:"bytes,8,rep,name=span_attributes,json=spanAttributes,proto3" json:"span_attributes,omitempty"`
	SpanName          string   `protobuf:"bytes,9,opt,name=span_name,json=spanName,proto3" json:"span_name,omitempty"`
	SpanStatus        string   `protobuf:"bytes,10,opt,name=span_status,json=spanStatus,proto3" json:"span_status,omitempty"`
	SpanStatusMessage string   `protobuf:"bytes,11,opt,name=span_status_message,json=spanStatusMessage,proto3" json:"span_status_message,omitempty"`
	SpanKind          string   `protobuf:"bytes,12,opt,name=span_kind,json=spanKind,proto3" json:"span_kind,omitempty"`
	EventName         []string `protobuf:"bytes,13,rep,name=event_name,json=eventName,proto3" json:"event_name,omitempty"`
	EventAttributes   []string `protobuf:"bytes,14,rep,name=event_attributes,json=eventAttributes,proto3" json:"event_attributes,omitempty"`
	LinkSpanId        []string `protobuf:"bytes,15,rep,name=link_span_id,json=linkSpanId,proto3" json:"link_span_id,omitempty"`
	LinkTraceId       []string `protobuf:"bytes,16,rep,name=link_trace_id,json=linkTraceId,proto3" json:"link_trace_id,omitempty"`
	LiknAttributes    []string `protobuf:"bytes,17,rep,name=likn_attributes,json=liknAttributes,proto3" json:"likn_attributes,omitempty"`
	TraceId           string   `protobuf:"bytes,18,opt,name=trace_id,json=traceId,proto3" json:"trace_id,omitempty"`
	SpanId            string   `protobuf:"bytes,19,opt,name=span_id,json=spanId,proto3" json:"span_id,omitempty"`
	TraceRootName     string   `protobuf:"bytes,20,opt,name=trace_root_name,json=traceRootName,proto3" json:"trace_root_name,omitempty"`
	TraceRootService  string   `protobuf:"bytes,22,opt,name=trace_root_service,json=traceRootService,proto3" json:"trace_root_service,omitempty"`
	ParentId          string   `protobuf:"bytes,23,opt,name=parent_id,json=parentId,proto3" json:"parent_id,omitempty"`
	SpanStartNano     int64    `protobuf:"varint,24,opt,name=span_start_nano,json=spanStartNano,proto3" json:"span_start_nano,omitempty"`
	SpanEndNano       int64    `protobuf:"varint,25,opt,name=span_end_nano,json=spanEndNano,proto3" json:"span_end_nano,omitempty"`
	TraceStartNano    int64    `protobuf:"varint,26,opt,name=trace_start_nano,json=traceStartNano,proto3" json:"trace_start_nano,omitempty"`
	TraceEndNano      int64    `protobuf:"varint,27,opt,name=trace_end_nano,json=traceEndNano,proto3" json:"trace_end_nano,omitempty"`
	TraceDuration     int64    `protobuf:"varint,28,opt,name=trace_duration,json=traceDuration,proto3" json:"trace_duration,omitempty"`
	SpanDuration      int64    `protobuf:"varint,29,opt,name=span_duration,json=spanDuration,proto3" json:"span_duration,omitempty"`
}

func (x *Span) Reset() {
	*x = Span{}
	if protoimpl.UnsafeEnabled {
		mi := &file_fri_v1_fri_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Span) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Span) ProtoMessage() {}

func (x *Span) ProtoReflect() protoreflect.Message {
	mi := &file_fri_v1_fri_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Span.ProtoReflect.Descriptor instead.
func (*Span) Descriptor() ([]byte, []int) {
	return file_fri_v1_fri_proto_rawDescGZIP(), []int{4}
}

func (x *Span) GetResource() []byte {
	if x != nil {
		return x.Resource
	}
	return nil
}

func (x *Span) GetResourceAttributes() []string {
	if x != nil {
		return x.ResourceAttributes
	}
	return nil
}

func (x *Span) GetScope() []byte {
	if x != nil {
		return x.Scope
	}
	return nil
}

func (x *Span) GetScopeAttributes() []string {
	if x != nil {
		return x.ScopeAttributes
	}
	return nil
}

func (x *Span) GetScopeName() string {
	if x != nil {
		return x.ScopeName
	}
	return ""
}

func (x *Span) GetScopeVersion() string {
	if x != nil {
		return x.ScopeVersion
	}
	return ""
}

func (x *Span) GetSpan() []byte {
	if x != nil {
		return x.Span
	}
	return nil
}

func (x *Span) GetSpanAttributes() []string {
	if x != nil {
		return x.SpanAttributes
	}
	return nil
}

func (x *Span) GetSpanName() string {
	if x != nil {
		return x.SpanName
	}
	return ""
}

func (x *Span) GetSpanStatus() string {
	if x != nil {
		return x.SpanStatus
	}
	return ""
}

func (x *Span) GetSpanStatusMessage() string {
	if x != nil {
		return x.SpanStatusMessage
	}
	return ""
}

func (x *Span) GetSpanKind() string {
	if x != nil {
		return x.SpanKind
	}
	return ""
}

func (x *Span) GetEventName() []string {
	if x != nil {
		return x.EventName
	}
	return nil
}

func (x *Span) GetEventAttributes() []string {
	if x != nil {
		return x.EventAttributes
	}
	return nil
}

func (x *Span) GetLinkSpanId() []string {
	if x != nil {
		return x.LinkSpanId
	}
	return nil
}

func (x *Span) GetLinkTraceId() []string {
	if x != nil {
		return x.LinkTraceId
	}
	return nil
}

func (x *Span) GetLiknAttributes() []string {
	if x != nil {
		return x.LiknAttributes
	}
	return nil
}

func (x *Span) GetTraceId() string {
	if x != nil {
		return x.TraceId
	}
	return ""
}

func (x *Span) GetSpanId() string {
	if x != nil {
		return x.SpanId
	}
	return ""
}

func (x *Span) GetTraceRootName() string {
	if x != nil {
		return x.TraceRootName
	}
	return ""
}

func (x *Span) GetTraceRootService() string {
	if x != nil {
		return x.TraceRootService
	}
	return ""
}

func (x *Span) GetParentId() string {
	if x != nil {
		return x.ParentId
	}
	return ""
}

func (x *Span) GetSpanStartNano() int64 {
	if x != nil {
		return x.SpanStartNano
	}
	return 0
}

func (x *Span) GetSpanEndNano() int64 {
	if x != nil {
		return x.SpanEndNano
	}
	return 0
}

func (x *Span) GetTraceStartNano() int64 {
	if x != nil {
		return x.TraceStartNano
	}
	return 0
}

func (x *Span) GetTraceEndNano() int64 {
	if x != nil {
		return x.TraceEndNano
	}
	return 0
}

func (x *Span) GetTraceDuration() int64 {
	if x != nil {
		return x.TraceDuration
	}
	return 0
}

func (x *Span) GetSpanDuration() int64 {
	if x != nil {
		return x.SpanDuration
	}
	return 0
}

var File_fri_v1_fri_proto protoreflect.FileDescriptor

var file_fri_v1_fri_proto_rawDesc = []byte{
	0x0a, 0x10, 0x66, 0x72, 0x69, 0x2f, 0x76, 0x31, 0x2f, 0x66, 0x72, 0x69, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x12, 0x02, 0x76, 0x31, 0x22, 0x8c, 0x01, 0x0a, 0x0d, 0x46, 0x69, 0x65, 0x6c, 0x64,
	0x56, 0x69, 0x65, 0x77, 0x49, 0x6e, 0x66, 0x6f, 0x12, 0x35, 0x0a, 0x06, 0x73, 0x68, 0x61, 0x72,
	0x64, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x1d, 0x2e, 0x76, 0x31, 0x2e, 0x46, 0x69,
	0x65, 0x6c, 0x64, 0x56, 0x69, 0x65, 0x77, 0x49, 0x6e, 0x66, 0x6f, 0x2e, 0x53, 0x68, 0x61, 0x72,
	0x64, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x06, 0x73, 0x68, 0x61, 0x72, 0x64, 0x73, 0x1a,
	0x44, 0x0a, 0x0b, 0x53, 0x68, 0x61, 0x72, 0x64, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x10,
	0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x04, 0x52, 0x03, 0x6b, 0x65, 0x79,
	0x12, 0x1f, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x09, 0x2e, 0x76, 0x31, 0x2e, 0x53, 0x68, 0x61, 0x72, 0x64, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75,
	0x65, 0x3a, 0x02, 0x38, 0x01, 0x22, 0x8a, 0x01, 0x0a, 0x05, 0x53, 0x68, 0x61, 0x72, 0x64, 0x12,
	0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x04, 0x52, 0x02, 0x69, 0x64, 0x12,
	0x34, 0x0a, 0x09, 0x62, 0x69, 0x74, 0x5f, 0x64, 0x65, 0x70, 0x74, 0x68, 0x18, 0x02, 0x20, 0x03,
	0x28, 0x0b, 0x32, 0x17, 0x2e, 0x76, 0x31, 0x2e, 0x53, 0x68, 0x61, 0x72, 0x64, 0x2e, 0x42, 0x69,
	0x74, 0x44, 0x65, 0x70, 0x74, 0x68, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x08, 0x62, 0x69, 0x74,
	0x44, 0x65, 0x70, 0x74, 0x68, 0x1a, 0x3b, 0x0a, 0x0d, 0x42, 0x69, 0x74, 0x44, 0x65, 0x70, 0x74,
	0x68, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x04, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75,
	0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x04, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02,
	0x38, 0x01, 0x22, 0x85, 0x01, 0x0a, 0x05, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x16, 0x0a, 0x06,
	0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x06, 0x73, 0x74,
	0x72, 0x65, 0x61, 0x6d, 0x12, 0x16, 0x0a, 0x06, 0x6c, 0x61, 0x62, 0x65, 0x6c, 0x73, 0x18, 0x02,
	0x20, 0x03, 0x28, 0x09, 0x52, 0x06, 0x6c, 0x61, 0x62, 0x65, 0x6c, 0x73, 0x12, 0x1c, 0x0a, 0x09,
	0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x03, 0x20, 0x01, 0x28, 0x04, 0x52,
	0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x12, 0x12, 0x0a, 0x04, 0x6c, 0x69,
	0x6e, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x04, 0x6c, 0x69, 0x6e, 0x65, 0x12, 0x1a,
	0x0a, 0x08, 0x6d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x18, 0x05, 0x20, 0x03, 0x28, 0x09,
	0x52, 0x08, 0x6d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x22, 0xf9, 0x01, 0x0a, 0x06, 0x53,
	0x61, 0x6d, 0x70, 0x6c, 0x65, 0x12, 0x16, 0x0a, 0x06, 0x73, 0x65, 0x72, 0x69, 0x65, 0x73, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x06, 0x73, 0x65, 0x72, 0x69, 0x65, 0x73, 0x12, 0x16, 0x0a,
	0x06, 0x6c, 0x61, 0x62, 0x65, 0x6c, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x09, 0x52, 0x06, 0x6c,
	0x61, 0x62, 0x65, 0x6c, 0x73, 0x12, 0x1c, 0x0a, 0x09, 0x65, 0x78, 0x65, 0x6d, 0x70, 0x6c, 0x61,
	0x72, 0x73, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x09, 0x65, 0x78, 0x65, 0x6d, 0x70, 0x6c,
	0x61, 0x72, 0x73, 0x12, 0x23, 0x0a, 0x04, 0x6b, 0x69, 0x6e, 0x64, 0x18, 0x04, 0x20, 0x01, 0x28,
	0x0e, 0x32, 0x0f, 0x2e, 0x76, 0x31, 0x2e, 0x53, 0x61, 0x6d, 0x70, 0x6c, 0x65, 0x2e, 0x4b, 0x69,
	0x6e, 0x64, 0x52, 0x04, 0x6b, 0x69, 0x6e, 0x64, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75,
	0x65, 0x18, 0x05, 0x20, 0x01, 0x28, 0x01, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x12, 0x1c,
	0x0a, 0x09, 0x68, 0x69, 0x73, 0x74, 0x6f, 0x67, 0x72, 0x61, 0x6d, 0x18, 0x06, 0x20, 0x01, 0x28,
	0x0c, 0x52, 0x09, 0x68, 0x69, 0x73, 0x74, 0x6f, 0x67, 0x72, 0x61, 0x6d, 0x12, 0x1c, 0x0a, 0x09,
	0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x07, 0x20, 0x01, 0x28, 0x03, 0x52,
	0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x22, 0x2a, 0x0a, 0x04, 0x4b, 0x69,
	0x6e, 0x64, 0x12, 0x08, 0x0a, 0x04, 0x4e, 0x4f, 0x4e, 0x45, 0x10, 0x00, 0x12, 0x09, 0x0a, 0x05,
	0x46, 0x4c, 0x4f, 0x41, 0x54, 0x10, 0x01, 0x12, 0x0d, 0x0a, 0x09, 0x48, 0x49, 0x53, 0x54, 0x4f,
	0x47, 0x52, 0x41, 0x4d, 0x10, 0x02, 0x22, 0xe8, 0x07, 0x0a, 0x04, 0x53, 0x70, 0x61, 0x6e, 0x12,
	0x1a, 0x0a, 0x08, 0x72, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x0c, 0x52, 0x08, 0x72, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x12, 0x2f, 0x0a, 0x13, 0x72,
	0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x5f, 0x61, 0x74, 0x74, 0x72, 0x69, 0x62, 0x75, 0x74,
	0x65, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x09, 0x52, 0x12, 0x72, 0x65, 0x73, 0x6f, 0x75, 0x72,
	0x63, 0x65, 0x41, 0x74, 0x74, 0x72, 0x69, 0x62, 0x75, 0x74, 0x65, 0x73, 0x12, 0x14, 0x0a, 0x05,
	0x73, 0x63, 0x6f, 0x70, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x05, 0x73, 0x63, 0x6f,
	0x70, 0x65, 0x12, 0x29, 0x0a, 0x10, 0x73, 0x63, 0x6f, 0x70, 0x65, 0x5f, 0x61, 0x74, 0x74, 0x72,
	0x69, 0x62, 0x75, 0x74, 0x65, 0x73, 0x18, 0x04, 0x20, 0x03, 0x28, 0x09, 0x52, 0x0f, 0x73, 0x63,
	0x6f, 0x70, 0x65, 0x41, 0x74, 0x74, 0x72, 0x69, 0x62, 0x75, 0x74, 0x65, 0x73, 0x12, 0x1d, 0x0a,
	0x0a, 0x73, 0x63, 0x6f, 0x70, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x05, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x09, 0x73, 0x63, 0x6f, 0x70, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x23, 0x0a, 0x0d,
	0x73, 0x63, 0x6f, 0x70, 0x65, 0x5f, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x18, 0x06, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x0c, 0x73, 0x63, 0x6f, 0x70, 0x65, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f,
	0x6e, 0x12, 0x12, 0x0a, 0x04, 0x73, 0x70, 0x61, 0x6e, 0x18, 0x07, 0x20, 0x01, 0x28, 0x0c, 0x52,
	0x04, 0x73, 0x70, 0x61, 0x6e, 0x12, 0x27, 0x0a, 0x0f, 0x73, 0x70, 0x61, 0x6e, 0x5f, 0x61, 0x74,
	0x74, 0x72, 0x69, 0x62, 0x75, 0x74, 0x65, 0x73, 0x18, 0x08, 0x20, 0x03, 0x28, 0x09, 0x52, 0x0e,
	0x73, 0x70, 0x61, 0x6e, 0x41, 0x74, 0x74, 0x72, 0x69, 0x62, 0x75, 0x74, 0x65, 0x73, 0x12, 0x1b,
	0x0a, 0x09, 0x73, 0x70, 0x61, 0x6e, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x09, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x08, 0x73, 0x70, 0x61, 0x6e, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x1f, 0x0a, 0x0b, 0x73,
	0x70, 0x61, 0x6e, 0x5f, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x18, 0x0a, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x0a, 0x73, 0x70, 0x61, 0x6e, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x2e, 0x0a, 0x13,
	0x73, 0x70, 0x61, 0x6e, 0x5f, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x5f, 0x6d, 0x65, 0x73, 0x73,
	0x61, 0x67, 0x65, 0x18, 0x0b, 0x20, 0x01, 0x28, 0x09, 0x52, 0x11, 0x73, 0x70, 0x61, 0x6e, 0x53,
	0x74, 0x61, 0x74, 0x75, 0x73, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x12, 0x1b, 0x0a, 0x09,
	0x73, 0x70, 0x61, 0x6e, 0x5f, 0x6b, 0x69, 0x6e, 0x64, 0x18, 0x0c, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x08, 0x73, 0x70, 0x61, 0x6e, 0x4b, 0x69, 0x6e, 0x64, 0x12, 0x1d, 0x0a, 0x0a, 0x65, 0x76, 0x65,
	0x6e, 0x74, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x0d, 0x20, 0x03, 0x28, 0x09, 0x52, 0x09, 0x65,
	0x76, 0x65, 0x6e, 0x74, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x29, 0x0a, 0x10, 0x65, 0x76, 0x65, 0x6e,
	0x74, 0x5f, 0x61, 0x74, 0x74, 0x72, 0x69, 0x62, 0x75, 0x74, 0x65, 0x73, 0x18, 0x0e, 0x20, 0x03,
	0x28, 0x09, 0x52, 0x0f, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x41, 0x74, 0x74, 0x72, 0x69, 0x62, 0x75,
	0x74, 0x65, 0x73, 0x12, 0x20, 0x0a, 0x0c, 0x6c, 0x69, 0x6e, 0x6b, 0x5f, 0x73, 0x70, 0x61, 0x6e,
	0x5f, 0x69, 0x64, 0x18, 0x0f, 0x20, 0x03, 0x28, 0x09, 0x52, 0x0a, 0x6c, 0x69, 0x6e, 0x6b, 0x53,
	0x70, 0x61, 0x6e, 0x49, 0x64, 0x12, 0x22, 0x0a, 0x0d, 0x6c, 0x69, 0x6e, 0x6b, 0x5f, 0x74, 0x72,
	0x61, 0x63, 0x65, 0x5f, 0x69, 0x64, 0x18, 0x10, 0x20, 0x03, 0x28, 0x09, 0x52, 0x0b, 0x6c, 0x69,
	0x6e, 0x6b, 0x54, 0x72, 0x61, 0x63, 0x65, 0x49, 0x64, 0x12, 0x27, 0x0a, 0x0f, 0x6c, 0x69, 0x6b,
	0x6e, 0x5f, 0x61, 0x74, 0x74, 0x72, 0x69, 0x62, 0x75, 0x74, 0x65, 0x73, 0x18, 0x11, 0x20, 0x03,
	0x28, 0x09, 0x52, 0x0e, 0x6c, 0x69, 0x6b, 0x6e, 0x41, 0x74, 0x74, 0x72, 0x69, 0x62, 0x75, 0x74,
	0x65, 0x73, 0x12, 0x19, 0x0a, 0x08, 0x74, 0x72, 0x61, 0x63, 0x65, 0x5f, 0x69, 0x64, 0x18, 0x12,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x74, 0x72, 0x61, 0x63, 0x65, 0x49, 0x64, 0x12, 0x17, 0x0a,
	0x07, 0x73, 0x70, 0x61, 0x6e, 0x5f, 0x69, 0x64, 0x18, 0x13, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06,
	0x73, 0x70, 0x61, 0x6e, 0x49, 0x64, 0x12, 0x26, 0x0a, 0x0f, 0x74, 0x72, 0x61, 0x63, 0x65, 0x5f,
	0x72, 0x6f, 0x6f, 0x74, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x14, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x0d, 0x74, 0x72, 0x61, 0x63, 0x65, 0x52, 0x6f, 0x6f, 0x74, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x2c,
	0x0a, 0x12, 0x74, 0x72, 0x61, 0x63, 0x65, 0x5f, 0x72, 0x6f, 0x6f, 0x74, 0x5f, 0x73, 0x65, 0x72,
	0x76, 0x69, 0x63, 0x65, 0x18, 0x16, 0x20, 0x01, 0x28, 0x09, 0x52, 0x10, 0x74, 0x72, 0x61, 0x63,
	0x65, 0x52, 0x6f, 0x6f, 0x74, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12, 0x1b, 0x0a, 0x09,
	0x70, 0x61, 0x72, 0x65, 0x6e, 0x74, 0x5f, 0x69, 0x64, 0x18, 0x17, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x08, 0x70, 0x61, 0x72, 0x65, 0x6e, 0x74, 0x49, 0x64, 0x12, 0x26, 0x0a, 0x0f, 0x73, 0x70, 0x61,
	0x6e, 0x5f, 0x73, 0x74, 0x61, 0x72, 0x74, 0x5f, 0x6e, 0x61, 0x6e, 0x6f, 0x18, 0x18, 0x20, 0x01,
	0x28, 0x03, 0x52, 0x0d, 0x73, 0x70, 0x61, 0x6e, 0x53, 0x74, 0x61, 0x72, 0x74, 0x4e, 0x61, 0x6e,
	0x6f, 0x12, 0x22, 0x0a, 0x0d, 0x73, 0x70, 0x61, 0x6e, 0x5f, 0x65, 0x6e, 0x64, 0x5f, 0x6e, 0x61,
	0x6e, 0x6f, 0x18, 0x19, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0b, 0x73, 0x70, 0x61, 0x6e, 0x45, 0x6e,
	0x64, 0x4e, 0x61, 0x6e, 0x6f, 0x12, 0x28, 0x0a, 0x10, 0x74, 0x72, 0x61, 0x63, 0x65, 0x5f, 0x73,
	0x74, 0x61, 0x72, 0x74, 0x5f, 0x6e, 0x61, 0x6e, 0x6f, 0x18, 0x1a, 0x20, 0x01, 0x28, 0x03, 0x52,
	0x0e, 0x74, 0x72, 0x61, 0x63, 0x65, 0x53, 0x74, 0x61, 0x72, 0x74, 0x4e, 0x61, 0x6e, 0x6f, 0x12,
	0x24, 0x0a, 0x0e, 0x74, 0x72, 0x61, 0x63, 0x65, 0x5f, 0x65, 0x6e, 0x64, 0x5f, 0x6e, 0x61, 0x6e,
	0x6f, 0x18, 0x1b, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0c, 0x74, 0x72, 0x61, 0x63, 0x65, 0x45, 0x6e,
	0x64, 0x4e, 0x61, 0x6e, 0x6f, 0x12, 0x25, 0x0a, 0x0e, 0x74, 0x72, 0x61, 0x63, 0x65, 0x5f, 0x64,
	0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x1c, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0d, 0x74,
	0x72, 0x61, 0x63, 0x65, 0x44, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x23, 0x0a, 0x0d,
	0x73, 0x70, 0x61, 0x6e, 0x5f, 0x64, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x1d, 0x20,
	0x01, 0x28, 0x03, 0x52, 0x0c, 0x73, 0x70, 0x61, 0x6e, 0x44, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f,
	0x6e, 0x42, 0x64, 0x0a, 0x06, 0x63, 0x6f, 0x6d, 0x2e, 0x76, 0x31, 0x42, 0x08, 0x46, 0x72, 0x69,
	0x50, 0x72, 0x6f, 0x74, 0x6f, 0x50, 0x01, 0x5a, 0x28, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e,
	0x63, 0x6f, 0x6d, 0x2f, 0x67, 0x65, 0x72, 0x6e, 0x65, 0x73, 0x74, 0x2f, 0x66, 0x72, 0x69, 0x65,
	0x72, 0x65, 0x6e, 0x2f, 0x67, 0x65, 0x6e, 0x2f, 0x67, 0x6f, 0x2f, 0x66, 0x72, 0x69, 0x2f, 0x76,
	0x31, 0xa2, 0x02, 0x03, 0x56, 0x58, 0x58, 0xaa, 0x02, 0x02, 0x56, 0x31, 0xca, 0x02, 0x02, 0x56,
	0x31, 0xe2, 0x02, 0x0e, 0x56, 0x31, 0x5c, 0x47, 0x50, 0x42, 0x4d, 0x65, 0x74, 0x61, 0x64, 0x61,
	0x74, 0x61, 0xea, 0x02, 0x02, 0x56, 0x31, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
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

var file_fri_v1_fri_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_fri_v1_fri_proto_msgTypes = make([]protoimpl.MessageInfo, 7)
var file_fri_v1_fri_proto_goTypes = []interface{}{
	(Sample_Kind)(0),      // 0: v1.Sample.Kind
	(*FieldViewInfo)(nil), // 1: v1.FieldViewInfo
	(*Shard)(nil),         // 2: v1.Shard
	(*Entry)(nil),         // 3: v1.Entry
	(*Sample)(nil),        // 4: v1.Sample
	(*Span)(nil),          // 5: v1.Span
	nil,                   // 6: v1.FieldViewInfo.ShardsEntry
	nil,                   // 7: v1.Shard.BitDepthEntry
}
var file_fri_v1_fri_proto_depIdxs = []int32{
	6, // 0: v1.FieldViewInfo.shards:type_name -> v1.FieldViewInfo.ShardsEntry
	7, // 1: v1.Shard.bit_depth:type_name -> v1.Shard.BitDepthEntry
	0, // 2: v1.Sample.kind:type_name -> v1.Sample.Kind
	2, // 3: v1.FieldViewInfo.ShardsEntry.value:type_name -> v1.Shard
	4, // [4:4] is the sub-list for method output_type
	4, // [4:4] is the sub-list for method input_type
	4, // [4:4] is the sub-list for extension type_name
	4, // [4:4] is the sub-list for extension extendee
	0, // [0:4] is the sub-list for field type_name
}

func init() { file_fri_v1_fri_proto_init() }
func file_fri_v1_fri_proto_init() {
	if File_fri_v1_fri_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_fri_v1_fri_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*FieldViewInfo); i {
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
			switch v := v.(*Shard); i {
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
			switch v := v.(*Entry); i {
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
			switch v := v.(*Sample); i {
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
		file_fri_v1_fri_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Span); i {
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
			NumEnums:      1,
			NumMessages:   7,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_fri_v1_fri_proto_goTypes,
		DependencyIndexes: file_fri_v1_fri_proto_depIdxs,
		EnumInfos:         file_fri_v1_fri_proto_enumTypes,
		MessageInfos:      file_fri_v1_fri_proto_msgTypes,
	}.Build()
	File_fri_v1_fri_proto = out.File
	file_fri_v1_fri_proto_rawDesc = nil
	file_fri_v1_fri_proto_goTypes = nil
	file_fri_v1_fri_proto_depIdxs = nil
}
