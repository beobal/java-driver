package com.datastax.driver.core.utils;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copyright (c) 2020 Apple, Inc. All rights reserved.
 */
public class FileUtils
{
    private static final Logger logger = LoggerFactory.getLogger(FileUtils.class);
    private static Class clsDirectBuffer;
    private static MethodHandle mhDirectBufferCleaner;
    private static MethodHandle mhCleanerClean;

    static
    {
        try
        {
            clsDirectBuffer = Class.forName("sun.nio.ch.DirectBuffer");
            Method mDirectBufferCleaner = clsDirectBuffer.getMethod("cleaner");
            mhDirectBufferCleaner = MethodHandles.lookup().unreflect(mDirectBufferCleaner);
            Method mCleanerClean = mDirectBufferCleaner.getReturnType().getMethod("clean");
            mhCleanerClean = MethodHandles.lookup().unreflect(mCleanerClean);

            ByteBuffer buf = ByteBuffer.allocateDirect(1);
            clean(buf);
        }
        catch (IllegalAccessException e)
        {
            logger.error("FATAL: Cassandra is unable to access required classes. This usually means it has been " +
                         "run without the aid of the standard startup scripts or the scripts have been edited. If this was " +
                         "intentional, and you are attempting to use Java 11+ you may need to add the --add-exports and " +
                         "--add-opens jvm options from either jvm11-server.options or jvm11-client.options");
            throw new RuntimeException(e);  // causes ExceptionInInitializerError, will prevent startup
        }
        catch (Throwable t)
        {
            logger.error("FATAL: Cannot initialize optimized memory deallocator.");
            throw new RuntimeException(t); // causes ExceptionInInitializerError, will prevent startup
        }
    }

    public static void clean(ByteBuffer buffer)
    {
        if (buffer == null || !buffer.isDirect())
            return;

        // TODO Once we can get rid of Java 8, it's simpler to call sun.misc.Unsafe.invokeCleaner(ByteBuffer),
        // but need to take care of the attachment handling (i.e. whether 'buf' is a duplicate or slice) - that
        // is different in sun.misc.Unsafe.invokeCleaner and this implementation.

        try
        {
            Object cleaner = mhDirectBufferCleaner.bindTo(buffer).invoke();
            if (cleaner != null)
            {
                // ((DirectBuffer) buf).cleaner().clean();
                mhCleanerClean.bindTo(cleaner).invoke();
            }
        }
        catch (RuntimeException e)
        {
            throw e;
        }
        catch (Throwable e)
        {
            throw new RuntimeException(e);
        }
    }

    public static String prettyPrintMemory(long size)
    {
        return prettyPrintMemory(size, false);
    }

    public static String prettyPrintMemory(long size, boolean includeSpace)
    {
        if (size >= 1 << 30)
            return String.format("%.3f%sGiB", size / (double) (1 << 30), includeSpace ? " " : "");
        if (size >= 1 << 20)
            return String.format("%.3f%sMiB", size / (double) (1 << 20), includeSpace ? " " : "");
        return String.format("%.3f%sKiB", size / (double) (1 << 10), includeSpace ? " " : "");
    }
}
