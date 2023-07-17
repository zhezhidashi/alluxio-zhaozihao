/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.underfs.tos;

import alluxio.underfs.ContentHashable;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;
import com.volcengine.tos.TOSV2;
import com.volcengine.tos.TosClientException;
import com.volcengine.tos.TosServerException;
import com.volcengine.tos.model.object.ObjectMetaRequestOptions;
import com.volcengine.tos.model.object.PutObjectInput;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A stream for writing a file into TOS. The data will be persisted to a temporary directory on the
 * local disk and copied as a complete file when the {@link #close()} method is called.
 */
@NotThreadSafe
public final class TOSOutputStream extends OutputStream implements ContentHashable {
  private static final Logger LOG = LoggerFactory.getLogger(TOSOutputStream.class);

  /** Bucket name of the Alluxio TOS bucket. */
  private final String mBucketName;
  /** Key of the file when it is uploaded to TOS. */
  private final String mKey;
  /** The local file that will be uploaded when the stream is closed. */
  private final File mFile;
  /** The tos client for TOS operations. */
  private final TOSV2 mTosClient;

  /** The output stream to a local file where the file will be buffered until closed. */
  private OutputStream mLocalOutputStream;
  /** The MD5 hash of the file. */
  private MessageDigest mHash;

  /** Flag to indicate this stream has been closed, to ensure close is only done once. */
  private AtomicBoolean mClosed = new AtomicBoolean(false);

  private String mContentHash;

  /**
   * Creates a name instance of {@link TOSOutputStream}.
   *
   * @param bucketName the name of the bucket
   * @param key the key of the file
   * @param client the client for TOS
   * @param tmpDirs a list of possible temporary directories
   */
  public TOSOutputStream(String bucketName, String key, TOSV2 client, List<String> tmpDirs)
      throws IOException {
    Preconditions.checkArgument(bucketName != null && !bucketName.isEmpty(),
        "Bucket name must not be null or empty.");
    Preconditions.checkArgument(key != null && !key.isEmpty(),
        "TOS path must not be null or empty.");
    Preconditions.checkArgument(client != null, "TOSClient must not be null.");
    mBucketName = bucketName;
    mKey = key;
    mTosClient = client;

    mFile = new File(PathUtils.concatPath(CommonUtils.getTmpDir(tmpDirs), UUID.randomUUID()));

    try {
      mHash = MessageDigest.getInstance("MD5");
      mLocalOutputStream =
          new BufferedOutputStream(new DigestOutputStream(new FileOutputStream(mFile), mHash));
    } catch (NoSuchAlgorithmException e) {
      LOG.warn("Algorithm not available for MD5 hash.", e);
      mHash = null;
      mLocalOutputStream = new BufferedOutputStream(new FileOutputStream(mFile));
    }
  }

  /**
   * Writes the given bytes to this output stream. Before close, the bytes are all written to local
   * file.
   *
   * @param b the bytes to write
   */
  @Override
  public void write(int b) throws IOException {
    mLocalOutputStream.write(b);
  }

  /**
   * Writes the given byte array to this output stream. Before close, the bytes are all written to
   * local file.
   *
   * @param b the byte array
   */
  @Override
  public void write(byte[] b) throws IOException {
    mLocalOutputStream.write(b, 0, b.length);
  }

  /**
   * Writes the given number of bytes from the given byte array starting at the given offset to this
   * output stream. Before close, the bytes are all written to local file.
   *
   * @param b the byte array
   * @param off the start offset in the data
   * @param len the number of bytes to write
   */
  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    mLocalOutputStream.write(b, off, len);
  }

  /**
   * Flushes this output stream and forces any buffered output bytes to be written out. Before
   * close, the data are flushed to local file.
   */
  @Override
  public void flush() throws IOException {
    mLocalOutputStream.flush();
  }

  /**
   * Closes this output stream. When an output stream is closed, the local temporary file is
   * uploaded to TOS Service. Once the file is uploaded, the temporary file is deleted.
   */
  @Override
  public void close() throws IOException {
    if (mClosed.getAndSet(true)) {
      return;
    }
    mLocalOutputStream.close();
    try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(mFile))) {
      ObjectMetaRequestOptions options = new ObjectMetaRequestOptions();
      options.setContentLength(mFile.length());
      if (mHash != null) {
        byte[] hashBytes = mHash.digest();
        options.setContentMD5(new String(Base64.encodeBase64(hashBytes)));
      }
      PutObjectInput input = new PutObjectInput().setBucket(mBucketName)
              .setKey(mKey).setContent(in).setOptions(options);
      mContentHash = mTosClient.putObject(input).getEtag();
    } catch (TosClientException e) {
      LOG.error("TOS Client Exception: Failed to upload {}. ", mKey);
      throw new IOException(e);
    } catch (TosServerException e) {
      LOG.error("TOS Server Exception: Failed to upload {}. ", mKey);
      throw new IOException(e);
    }
    finally {
      // Delete the temporary file on the local machine if the TOS client completed the
      // upload or if the upload failed.
      if (!mFile.delete()) {
        LOG.error("Failed to delete temporary file @ {}", mFile.getPath());
      }
    }
    return;
  }

  @Override
  public Optional<String> getContentHash() {
    return Optional.ofNullable(mContentHash);
  }
}