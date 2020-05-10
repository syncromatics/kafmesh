// Code generated by protoc-gen-go. DO NOT EDIT.
// source: kafmesh/watch/v1/watch_api.proto

package watchv1

import (
	context "context"
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	grpc "google.golang.org/grpc"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type ProcessorRequest struct {
	Component            string   `protobuf:"bytes,1,opt,name=component,proto3" json:"component,omitempty"`
	Processor            string   `protobuf:"bytes,2,opt,name=processor,proto3" json:"processor,omitempty"`
	Key                  string   `protobuf:"bytes,3,opt,name=key,proto3" json:"key,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ProcessorRequest) Reset()         { *m = ProcessorRequest{} }
func (m *ProcessorRequest) String() string { return proto.CompactTextString(m) }
func (*ProcessorRequest) ProtoMessage()    {}
func (*ProcessorRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_c576eeab6cb310d3, []int{0}
}

func (m *ProcessorRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ProcessorRequest.Unmarshal(m, b)
}
func (m *ProcessorRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ProcessorRequest.Marshal(b, m, deterministic)
}
func (m *ProcessorRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ProcessorRequest.Merge(m, src)
}
func (m *ProcessorRequest) XXX_Size() int {
	return xxx_messageInfo_ProcessorRequest.Size(m)
}
func (m *ProcessorRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_ProcessorRequest.DiscardUnknown(m)
}

var xxx_messageInfo_ProcessorRequest proto.InternalMessageInfo

func (m *ProcessorRequest) GetComponent() string {
	if m != nil {
		return m.Component
	}
	return ""
}

func (m *ProcessorRequest) GetProcessor() string {
	if m != nil {
		return m.Processor
	}
	return ""
}

func (m *ProcessorRequest) GetKey() string {
	if m != nil {
		return m.Key
	}
	return ""
}

type ProcessorResponse struct {
	Operation            *Operation `protobuf:"bytes,1,opt,name=operation,proto3" json:"operation,omitempty"`
	XXX_NoUnkeyedLiteral struct{}   `json:"-"`
	XXX_unrecognized     []byte     `json:"-"`
	XXX_sizecache        int32      `json:"-"`
}

func (m *ProcessorResponse) Reset()         { *m = ProcessorResponse{} }
func (m *ProcessorResponse) String() string { return proto.CompactTextString(m) }
func (*ProcessorResponse) ProtoMessage()    {}
func (*ProcessorResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_c576eeab6cb310d3, []int{1}
}

func (m *ProcessorResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ProcessorResponse.Unmarshal(m, b)
}
func (m *ProcessorResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ProcessorResponse.Marshal(b, m, deterministic)
}
func (m *ProcessorResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ProcessorResponse.Merge(m, src)
}
func (m *ProcessorResponse) XXX_Size() int {
	return xxx_messageInfo_ProcessorResponse.Size(m)
}
func (m *ProcessorResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_ProcessorResponse.DiscardUnknown(m)
}

var xxx_messageInfo_ProcessorResponse proto.InternalMessageInfo

func (m *ProcessorResponse) GetOperation() *Operation {
	if m != nil {
		return m.Operation
	}
	return nil
}

func init() {
	proto.RegisterType((*ProcessorRequest)(nil), "kafmesh.watch.v1.ProcessorRequest")
	proto.RegisterType((*ProcessorResponse)(nil), "kafmesh.watch.v1.ProcessorResponse")
}

func init() { proto.RegisterFile("kafmesh/watch/v1/watch_api.proto", fileDescriptor_c576eeab6cb310d3) }

var fileDescriptor_c576eeab6cb310d3 = []byte{
	// 257 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0x52, 0xc8, 0x4e, 0x4c, 0xcb,
	0x4d, 0x2d, 0xce, 0xd0, 0x2f, 0x4f, 0x2c, 0x49, 0xce, 0xd0, 0x2f, 0x33, 0x84, 0x30, 0xe2, 0x13,
	0x0b, 0x32, 0xf5, 0x0a, 0x8a, 0xf2, 0x4b, 0xf2, 0x85, 0x04, 0xa0, 0x2a, 0xf4, 0xc0, 0x12, 0x7a,
	0x65, 0x86, 0x52, 0x98, 0x7a, 0xf2, 0x0b, 0x52, 0x8b, 0x12, 0x4b, 0x32, 0xf3, 0xf3, 0x20, 0x7a,
	0x94, 0x12, 0xb8, 0x04, 0x02, 0x8a, 0xf2, 0x93, 0x53, 0x8b, 0x8b, 0xf3, 0x8b, 0x82, 0x52, 0x0b,
	0x4b, 0x53, 0x8b, 0x4b, 0x84, 0x64, 0xb8, 0x38, 0x93, 0xf3, 0x73, 0x0b, 0xf2, 0xf3, 0x52, 0xf3,
	0x4a, 0x24, 0x18, 0x15, 0x18, 0x35, 0x38, 0x83, 0x10, 0x02, 0x20, 0xd9, 0x02, 0x98, 0x0e, 0x09,
	0x26, 0x88, 0x2c, 0x5c, 0x40, 0x48, 0x80, 0x8b, 0x39, 0x3b, 0xb5, 0x52, 0x82, 0x19, 0x2c, 0x0e,
	0x62, 0x2a, 0xf9, 0x71, 0x09, 0x22, 0xd9, 0x50, 0x5c, 0x90, 0x9f, 0x57, 0x9c, 0x2a, 0x64, 0xc9,
	0xc5, 0x09, 0x77, 0x09, 0xd8, 0x0a, 0x6e, 0x23, 0x69, 0x3d, 0x74, 0xe7, 0xeb, 0xf9, 0xc3, 0x94,
	0x04, 0x21, 0x54, 0x1b, 0x25, 0x71, 0x71, 0x84, 0x83, 0x14, 0x38, 0x06, 0x78, 0x0a, 0x85, 0x71,
	0x71, 0xc2, 0xcd, 0x16, 0x52, 0xc2, 0x34, 0x00, 0xdd, 0x6b, 0x52, 0xca, 0x78, 0xd5, 0x40, 0x1c,
	0x67, 0xc0, 0xe8, 0xe4, 0xc9, 0x25, 0x92, 0x9c, 0x9f, 0x8b, 0xa1, 0xd6, 0x89, 0x17, 0x62, 0x73,
	0x41, 0x66, 0x00, 0x28, 0xf0, 0x02, 0x18, 0xa3, 0xd8, 0xc1, 0x52, 0x65, 0x86, 0x8b, 0x98, 0x98,
	0xbd, 0xc3, 0x23, 0x56, 0x31, 0x09, 0x78, 0x43, 0xb5, 0x80, 0x15, 0xea, 0x85, 0x19, 0x26, 0xb1,
	0x81, 0xc3, 0xd9, 0x18, 0x10, 0x00, 0x00, 0xff, 0xff, 0xb5, 0x7e, 0x20, 0xcf, 0xbf, 0x01, 0x00,
	0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// WatchAPIClient is the client API for WatchAPI service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type WatchAPIClient interface {
	// Processor will return operations from a processor based on key.
	Processor(ctx context.Context, in *ProcessorRequest, opts ...grpc.CallOption) (WatchAPI_ProcessorClient, error)
}

type watchAPIClient struct {
	cc *grpc.ClientConn
}

func NewWatchAPIClient(cc *grpc.ClientConn) WatchAPIClient {
	return &watchAPIClient{cc}
}

func (c *watchAPIClient) Processor(ctx context.Context, in *ProcessorRequest, opts ...grpc.CallOption) (WatchAPI_ProcessorClient, error) {
	stream, err := c.cc.NewStream(ctx, &_WatchAPI_serviceDesc.Streams[0], "/kafmesh.watch.v1.WatchAPI/Processor", opts...)
	if err != nil {
		return nil, err
	}
	x := &watchAPIProcessorClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type WatchAPI_ProcessorClient interface {
	Recv() (*ProcessorResponse, error)
	grpc.ClientStream
}

type watchAPIProcessorClient struct {
	grpc.ClientStream
}

func (x *watchAPIProcessorClient) Recv() (*ProcessorResponse, error) {
	m := new(ProcessorResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// WatchAPIServer is the server API for WatchAPI service.
type WatchAPIServer interface {
	// Processor will return operations from a processor based on key.
	Processor(*ProcessorRequest, WatchAPI_ProcessorServer) error
}

func RegisterWatchAPIServer(s *grpc.Server, srv WatchAPIServer) {
	s.RegisterService(&_WatchAPI_serviceDesc, srv)
}

func _WatchAPI_Processor_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(ProcessorRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(WatchAPIServer).Processor(m, &watchAPIProcessorServer{stream})
}

type WatchAPI_ProcessorServer interface {
	Send(*ProcessorResponse) error
	grpc.ServerStream
}

type watchAPIProcessorServer struct {
	grpc.ServerStream
}

func (x *watchAPIProcessorServer) Send(m *ProcessorResponse) error {
	return x.ServerStream.SendMsg(m)
}

var _WatchAPI_serviceDesc = grpc.ServiceDesc{
	ServiceName: "kafmesh.watch.v1.WatchAPI",
	HandlerType: (*WatchAPIServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Processor",
			Handler:       _WatchAPI_Processor_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "kafmesh/watch/v1/watch_api.proto",
}
