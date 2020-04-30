package com.datastax.driver.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.google.common.primitives.Ints;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Copyright (c) 2020 Apple, Inc. All rights reserved.
 */
public class MessageProcessor extends ChannelInboundHandlerAdapter implements FrameDecoder.FrameProcessor
{
    private static final Logger logger = LoggerFactory.getLogger(MessageProcessor.class);

    private final int largeThreshold = PayloadEncoder.MAX_FRAME_SIZE;
    private LargeMessage largeMessage;

    private final FrameDecoder decoder;
    private final Frame.Decoder cqlFrameDecoder;
    private final Message.ProtocolDecoder messageDecoder;
    private final Connection.Dispatcher dispatcher;
    private ChannelHandlerContext ctx;

    MessageProcessor(FrameDecoder decoder,
                     Frame.Decoder cqlFrameDecoder,
                     Message.ProtocolDecoder messageDecoder,
                     Connection.Dispatcher dispatcher)
    {
        this.decoder = decoder;
        this.cqlFrameDecoder = cqlFrameDecoder;
        this.messageDecoder = messageDecoder;
        this.dispatcher = dispatcher;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx)
    {
        this.ctx = ctx;
        decoder.activate(this); // the frame decoder starts inactive until explicitly activated by the added inbound message handler
    }

    public boolean process(FrameDecoder.Frame frame) throws IOException
    {
        if (frame instanceof FrameDecoder.IntactFrame)
            return processIntactFrame((FrameDecoder.IntactFrame) frame);

        processCorruptFrame((FrameDecoder.CorruptFrame) frame);
        return true;
    }

    private boolean processIntactFrame(FrameDecoder.IntactFrame frame) throws IOException
    {
        if (frame.isSelfContained)
            return processFrameOfContainedMessages(frame.contents);
        else if (null == largeMessage)
            return processFirstFrameOfLargeMessage(frame);
        else
            return processSubsequentFrameOfLargeMessage(frame);
    }


    /*
     * Handle contained messages (not crossing boundaries of the frame) - both small and large, for the inbound
     * definition of large (breaching the size threshold for what we are willing to process on event-loop vs.
     * off event-loop).
     */
    private boolean processFrameOfContainedMessages(ShareableBytes bytes) throws IOException
    {
        int count = 0;
        long size = bytes.remaining();
        while (bytes.hasRemaining())
        {
            if (!processOneContainedMessage(bytes))
                return false;
            count++;
        }
        return true;
    }

    private boolean processOneContainedMessage(ShareableBytes bytes) throws IOException
    {
        Frame frame = deserialize(bytes);
        // TODO ??
        if (frame == null)
            return false;

        // todo make this a long?
        int size = Ints.checkedCast(frame.body.readableBytes());

        if (size <= largeThreshold)
        {
            processCqlFrame(frame);
        }
        else
        {
//            processLargeMessage(bytes, size, header);
        }

        return true;
    }

    private boolean processFirstFrameOfLargeMessage(FrameDecoder.IntactFrame frame) throws IOException
    {
        ShareableBytes bytes = frame.contents;
        ByteBuf buf = Unpooled.wrappedBuffer(bytes.get());
        try
        {
            // get the header
            int versionBytes = buf.readByte();
            // version first byte is the "direction" of the frame (request or response)
            ProtocolVersion version = ProtocolVersion.fromInt(versionBytes & 0x7F);
            int flags = buf.readByte();
            int streamId = buf.readShort();
            int opcode = buf.readByte();
            int length = buf.readInt();
            Frame.Header header = new Frame.Header(version, flags, streamId, opcode);
            largeMessage = new LargeMessage(header, length);
            largeMessage.supply(frame);
            return true;
        }
        catch (Exception e)
        {
            throw new IOException("Error decoding CQL frame", e);
        }
    }

    private boolean processSubsequentFrameOfLargeMessage(FrameDecoder.Frame frame)
    {
        if (largeMessage.supply(frame))
        {
            largeMessage = null;
        }
        return true;
    }

    private Frame deserialize(ShareableBytes bytes)
    {
        ByteBuffer buf = bytes.get();
        final int begin = buf.position();
        final int end = buf.limit();
        ByteBuf buffer = Unpooled.wrappedBuffer(buf);

        try
        {
            Frame f = (Frame)cqlFrameDecoder.decodeFrame(ctx, buffer);
            buf.position(begin + buffer.readerIndex());
            return f;
        }
        catch (Throwable t)
        {
            logger.error("{} unexpected exception caught while deserializing a message", ctx.channel().toString(), t);
        }
        finally
        {
            // no matter what, set position to the beginning of the next message and restore limit, so that
            // we can always keep on decoding the frame even on failure to deserialize previous message
            buf.limit(end);
        }

        return null;
    }

    private void processCqlFrame(Frame frame)
    {
        Message.Response message = messageDecoder.decodeMessage(ctx, frame);
        dispatcher.dispatch(ctx, message);
    }

    private void processCorruptFrame(FrameDecoder.CorruptFrame frame) throws Crc.InvalidCrc
    {
        if (!frame.isRecoverable())
        {
            throw new Crc.InvalidCrc(frame.readCRC, frame.computedCRC);
        }
        else if (frame.isSelfContained)
        {
            logger.warn("{} invalid, recoverable CRC mismatch detected while reading messages (corrupted self-contained frame)", ctx.channel().toString());
        }
        else if (null == largeMessage) // first frame of a large message
        {
            logger.warn("{} invalid, unrecoverable CRC mismatch detected while reading messages (corrupted first frame of a large message)", ctx.channel().toString());
            throw new Crc.InvalidCrc(frame.readCRC, frame.computedCRC);
        }
        else // subsequent frame of a large message
        {
            processSubsequentFrameOfLargeMessage(frame);
            logger.warn("{} invalid, recoverable CRC mismatch detected while reading a large message", ctx.channel().toString());
        }
    }

    private class LargeMessage
    {
        private final int size;
        private final Frame.Header header;

        private final List<ShareableBytes> buffers = new ArrayList<>();
        private long received;

        private boolean isCorrupt;


        private LargeMessage(Frame.Header header, int size)
        {
            this.header = header;
            this.size = size;
        }

        private void schedule()
        {
            processCqlFrame(assembleFrame());
            releaseBuffers();
        }

        private Frame assembleFrame()
        {
            // TODO we already have the frame header, so we could skip HEADER_LENGTH
            // and go straight to message decoding
            ByteBuf concat = Unpooled.wrappedBuffer(buffers.stream()
                                                           .map(ShareableBytes::get)
                                                           .toArray(ByteBuffer[]::new));
            try
            {
                return (Frame)cqlFrameDecoder.decodeFrame(ctx, concat);
            } catch (Exception e)
            {
                buffers.forEach(ShareableBytes::release);
                throw new RuntimeException("Error deserializing CQL frame", e);
            }
            //TODO lifecycle
        }

        /**
         * Return true if this was the last frame of the large message.
         */
        private boolean supply(FrameDecoder.Frame frame)
        {
            if (frame instanceof FrameDecoder.IntactFrame)
                onIntactFrame((FrameDecoder.IntactFrame) frame);
            else
                onCorruptFrame();

            received += frame.frameSize;
            // we include the header in the initial buffer on the driver side
            if (size == received - Frame.Header.lengthFor(ProtocolVersion.V5))
                onComplete();
            return size == received;
        }

        private void onIntactFrame(FrameDecoder.IntactFrame frame)
        {
            if (!isCorrupt)
            {
                buffers.add(frame.contents.sliceAndConsume(frame.frameSize).share());
                return;
            }
            frame.consume();
        }

        private void onCorruptFrame()
        {
            if (!isCorrupt)
                releaseBuffersAndCapacity(); // release resources once we transition from normal state to corrupt
            isCorrupt = true;
        }

        private void onComplete()
        {
            if (!isCorrupt)
                schedule();
        }

        private void abort()
        {
            if (!isCorrupt)
                releaseBuffersAndCapacity(); // release resources if in normal state when abort() is invoked
        }

        private void releaseBuffers()
        {
            buffers.forEach(ShareableBytes::release); buffers.clear();
        }

        private void releaseBuffersAndCapacity()
        {
            releaseBuffers();
//            releaseCapacity(size);
        }
    }
}
