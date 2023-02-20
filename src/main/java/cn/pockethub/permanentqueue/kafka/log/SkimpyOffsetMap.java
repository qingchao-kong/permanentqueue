package cn.pockethub.permanentqueue.kafka.log;

import cn.pockethub.permanentqueue.kafka.utils.CoreUtils;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * An hash table used for deduplicating the log. This hash table uses a cryptographicly secure hash of the key as a proxy for the key
 * for comparisons and to save space on object overhead. Collisions are resolved by probing. This hash table does not support deletes.
 */
//@nonthreadsafe
public class SkimpyOffsetMap implements OffsetMap {

    private Integer memory;
    private String hashAlgorithm;

    private ByteBuffer bytes;

    /* the hash algorithm instance to use, default is MD5 */
    private MessageDigest digest;

    /* the number of bytes for this hash algorithm */
    private int hashSize;

    /* create some hash buffers to avoid reallocating each time */
    private byte[] hash1;
    private byte[] hash2;

    /* number of entries put into the map */
    private int entries = 0;

    /* number of lookups on the map */
    private long lookups = 0L;

    /* the number of probes for all lookups */
    private long probes = 0L;

    /* the latest offset written into the map */
    private long lastOffset = -1L;

    /**
     * The number of bytes of space each entry uses (the number of bytes in the hash plus an 8 byte offset)
     */
    private int bytesPerEntry;

    /**
     * @param memory        The amount of memory this map can use
     * @param hashAlgorithm The hash algorithm instance to use: MD2, MD5, SHA-1, SHA-256, SHA-384, SHA-512
     * @throws NoSuchAlgorithmException
     */
    public SkimpyOffsetMap(Integer memory, String hashAlgorithm) throws NoSuchAlgorithmException {
        this.memory = memory;
        this.hashAlgorithm = hashAlgorithm;

        this.digest = MessageDigest.getInstance(hashAlgorithm);
        this.hashSize = digest.getDigestLength();
        this.hash1 = new byte[hashSize];
        this.hash2 = new byte[hashSize];
        this.bytesPerEntry = hashSize + 8;

        this.bytes = ByteBuffer.allocate(memory);
    }

    /**
     * The maximum number of entries this map can contain
     */
    @Override
    public Integer slots() {
        return memory / bytesPerEntry;
    }

    /**
     * Associate this offset to the given key.
     *
     * @param key    The key
     * @param offset The offset
     */
    @Override
    public void put(ByteBuffer key, Long offset) throws DigestException {
        assert entries < slots() : "Attempt to add a new entry to a full offset map.";
        lookups += 1;
        hashInto(key, hash1);
        // probe until we find the first empty slot
        int attempt = 0;
        int pos = positionOf(hash1, attempt);
        while (!isEmpty(pos)) {
            bytes.position(pos);
            bytes.get(hash2);
            if (Arrays.equals(hash1, hash2)) {
                // we found an existing entry, overwrite it and return (size does not change)
                bytes.putLong(offset);
                lastOffset = offset;
                return;
            }
            attempt += 1;
            pos = positionOf(hash1, attempt);
        }
        // found an empty slot, update it--size grows by 1
        bytes.position(pos);
        bytes.put(hash1);
        bytes.putLong(offset);
        lastOffset = offset;
        entries += 1;
    }

    /**
     * Check that there is no entry at the given position
     */
    private Boolean isEmpty(Integer position) {
        return bytes.getLong(position) == 0 && bytes.getLong(position + 8) == 0 && bytes.getLong(position + 16) == 0;
    }

    /**
     * Get the offset associated with this key.
     *
     * @param key The key
     * @return The offset associated with this key or -1 if the key is not found
     */
    @Override
    public Long get(ByteBuffer key) throws DigestException {
        lookups += 1;
        hashInto(key, hash1);
        // search for the hash of this key by repeated probing until we find the hash we are looking for or we find an empty slot
        int attempt = 0;
        int pos = 0;
        //we need to guard against attempt integer overflow if the map is full
        //limit attempt to number of slots once positionOf(..) enters linear search mode
        int maxAttempts = slots() + hashSize - 4;
        do {
            if (attempt >= maxAttempts) {
                return -1L;
            }
            pos = positionOf(hash1, attempt);
            bytes.position(pos);
            if (isEmpty(pos)) {
                return -1L;
            }
            bytes.get(hash2);
            attempt += 1;
        } while (!Arrays.equals(hash1, hash2));
        return bytes.getLong();
    }

    /**
     * Change the salt used for key hashing making all existing keys unfindable.
     */
    @Override
    public void clear() {
        this.entries = 0;
        this.lookups = 0L;
        this.probes = 0L;
        this.lastOffset = -1L;
        Arrays.fill(bytes.array(), bytes.arrayOffset(), bytes.arrayOffset() + bytes.limit(), new Integer(0).byteValue());
    }

    /**
     * The number of entries put into the map (note that not all may remain)
     */
    @Override
    public Integer size() {
        return entries;
    }

    /**
     * The rate of collisions in the lookups
     *
     * @return
     */
    public double collisionRate() {
        return (this.probes - this.lookups) / (double) this.lookups;
    }

    /**
     * The latest offset put into the map
     */
    @Override
    public Long latestOffset() {
        return lastOffset;
    }

    @Override
    public void updateLatestOffset(Long offset) {
        lastOffset = offset;
    }

    /**
     * Calculate the ith probe position. We first try reading successive integers from the hash itself
     * then if all of those fail we degrade to linear probing.
     *
     * @param hash    The hash of the key to find the position for
     * @param attempt The ith probe
     * @return The byte offset in the buffer at which the ith probing for the given hash would reside
     */
    private Integer positionOf(byte[] hash, Integer attempt) {
        int probe = CoreUtils.readInt(hash, Math.min(attempt, hashSize - 4)) + Math.max(0, attempt - hashSize + 4);
        int slot = Utils.abs(probe) % slots();
        this.probes += 1;
        return slot * bytesPerEntry;
    }

    /**
     * The offset at which we have stored the given key
     *
     * @param key    The key to hash
     * @param buffer The buffer to store the hash into
     */
    private void hashInto(ByteBuffer key, byte[] buffer) throws DigestException {
        key.mark();
        digest.update(key);
        key.reset();
        digest.digest(buffer, 0, hashSize);
    }
}
