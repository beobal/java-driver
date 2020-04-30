package com.datastax.driver.core;

import java.nio.ByteBuffer;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

/**
 * Copyright (c) 2020 Apple, Inc. All rights reserved.
 */
public final class PayloadEncoder extends ChannelOutboundHandlerAdapter
{
    public static final int MAX_FRAME_SIZE = 1 << 17;
    private final FrameEncoder.PayloadAllocator allocator;
    private FrameEncoder.Payload sending;
    public PayloadEncoder(FrameEncoder.PayloadAllocator allocator)
    {
        this.allocator = allocator;
    }

    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception
    {
        // poll a queue of messages
        // loop until a single frame is filled or we run out of messages
        // then write/flush
        if (null == sending)
            sending = allocator.allocate(true, MAX_FRAME_SIZE);

        Frame outbound = (Frame)msg;
        ProtocolVersion protocolVersion = outbound.header.version;
        int headerLength = Frame.Header.lengthFor(protocolVersion);
        if(sending.remaining() < headerLength + outbound.body.readableBytes())
        {
            sending.finish();
            ctx.write(sending);
            sending.release();
            sending = allocator.allocate(true, MAX_FRAME_SIZE);
        }

        ByteBuffer buf = sending.buffer;
        buf.put((byte)protocolVersion.toInt());
        buf.put((byte) Frame.Header.Flag.serialize(outbound.header.flags));
        buf.putShort((short)outbound.header.streamId);

        buf.put((byte)outbound.header.opcode);
        buf.putInt(outbound.body.readableBytes());
        buf.put(outbound.body.nioBuffer());

        // TODO scheduling
        sending.finish();
        ctx.write(sending);
        sending.release();
        sending = null;
    }
}
