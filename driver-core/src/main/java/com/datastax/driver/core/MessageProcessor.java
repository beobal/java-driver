package com.datastax.driver.core;

import java.io.IOException;
import java.nio.ByteBuffer;

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

    private final int largeThreshold =  1024 * 1024 * 256;

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

        throw new IllegalArgumentException("Large message support not added yet");
//        else if (null == largeMessage)
//            return processFirstFrameOfLargeMessage(frame, endpointReserve, globalReserve);
//        else
//            return processSubsequentFrameOfLargeMessage(frame);
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
        logger.warn("XXX PROCESSED FRAME OF {} MESSAGES ({})", count, size);
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
            throw new IllegalArgumentException("Large message support not yet added");
//            processLargeMessage(bytes, size, header);
        }

        return true;
    }

    private Frame deserialize(ShareableBytes bytes)
    {
        ByteBuffer buf = bytes.get();
        final int begin = buf.position();
        final int end = buf.limit();
        ByteBuf buffer = Unpooled.wrappedBuffer(buf);

        // get o.a.c.transport.Frame.Header from buf
        // deserialize o.a.c.transport.Message from buf
        // create task to process Header + Message
        try
        {
            Frame f = (Frame)cqlFrameDecoder.decodeFrame(ctx, buffer);
            buf.position(begin + buffer.readerIndex());
            return f;
        }
        catch (Throwable t)
        {
//            callbacks.onFailedDeserialize(size, header, t);
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
        logger.info("Processing {}, s={}, v={}", message.type, frame.header.streamId, frame.header.version);
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
//        else if (null == largeMessage) // first frame of a large message
//        {
//            receivedBytes += frame.frameSize;
//            corruptFramesUnrecovered++;
//            noSpamLogger.error("{} invalid, unrecoverable CRC mismatch detected while reading messages (corrupted first frame of a large message)", id());
//            throw new Crc.InvalidCrc(frame.readCRC, frame.computedCRC);
//        }
//        else // subsequent frame of a large message
//        {
//            processSubsequentFrameOfLargeMessage(frame);
//            corruptFramesRecovered++;
//            noSpamLogger.warn("{} invalid, recoverable CRC mismatch detected while reading a large message", id());
//        }
    }
}
