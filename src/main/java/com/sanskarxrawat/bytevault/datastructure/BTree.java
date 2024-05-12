package com.sanskarxrawat.bytevault.datastructure;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;


import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BTree<K extends Comparable<K>, V> implements AutoCloseable {
    private static final int ORDER = 4;
    private final File indexFile;
    private final RandomAccessFile raf;
    private Node<K, V> root;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public BTree(File indexFile) {
        this.indexFile = indexFile;
        try {
            this.raf = new RandomAccessFile(indexFile, "rw");
            this.root = loadRoot();
        } catch (IOException e) {
            throw new RuntimeException("Error initializing B-Tree", e);
        }
    }

    public void insert(K key, V value) throws IOException {
        lock.writeLock().lock();
        try {
            if (root == null) {
                root = new Node<>(true);
                root.keys[0] = key;
                root.values[0] = value;
                root.numKeys = 1;
            } else {
                if (root.numKeys == 2 * ORDER - 1) {
                    Node<K, V> newRoot = new Node<>(false);
                    newRoot.children[0] = root;
                    newRoot.splitChild(0, root);
                    root = newRoot;
                }
                insertNonFull(root, key, value);
            }
            saveRoot(root);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void insertNonFull(Node<K, V> x, K key, V value) {
        int i = x.numKeys - 1;
        if (x.isLeaf) {
            while (i >= 0 && key.compareTo(x.keys[i]) < 0) {
                x.keys[i + 1] = x.keys[i];
                x.values[i + 1] = x.values[i];
                i--;
            }
            x.keys[i + 1] = key;
            x.values[i + 1] = value;
            x.numKeys++;
        } else {
            while (i >= 0 && key.compareTo(x.keys[i]) < 0) {
                i--;
            }
            i++;
            if (x.children[i].numKeys == 2 * ORDER - 1) {
                x.splitChild(i, x.children[i]);
                if (key.compareTo(x.keys[i]) > 0) {
                    i++;
                }
            }
            insertNonFull(x.children[i], key, value);
        }
    }

    public V get(K key) {
        lock.readLock().lock();
        try {
            return getRecursive(root, key);
        } finally {
            lock.readLock().unlock();
        }
    }

    private V getRecursive(Node<K, V> x, K key) {
        int i = 0;
        while (i < x.numKeys && key.compareTo(x.keys[i]) > 0) {
            i++;
        }
        if (i < x.numKeys && key.compareTo(x.keys[i]) == 0) {
            return x.values[i];
        } else if (x.isLeaf) {
            return null;
        } else {
            return getRecursive(x.children[i], key);
        }
    }

    public void delete(K key) {
        lock.writeLock().lock();
        try {
            deleteRecursive(root, key);
            saveRoot(root);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void deleteRecursive(Node<K, V> x, K key) {
        int i = 0;
        while (i < x.numKeys && key.compareTo(x.keys[i]) > 0) {
            i++;
        }
        if (i < x.numKeys && key.compareTo(x.keys[i]) == 0) {
            if (x.isLeaf) {
                deleteFromLeaf(x, i);
            } else {
                deleteFromNonLeaf(x, i);
            }
        } else if (!x.isLeaf) {
            if (x.children[i].numKeys < ORDER) {
                fillNode(x, i);
            }
            deleteRecursive(x.children[i], key);
        }
    }

    private void deleteFromLeaf(Node<K, V> x, int i) {
        for (int j = i + 1; j < x.numKeys; j++) {
            x.keys[j - 1] = x.keys[j];
            x.values[j - 1] = x.values[j];
        }
        x.numKeys--;
    }

    private void deleteFromNonLeaf(Node<K, V> x, int i) {
        K key = x.keys[i];
        if (x.children[i].numKeys >= ORDER) {
            x.keys[i] = getPredecessor(x.children[i]);
            x.values[i] = getValueForKey(x.children[i], x.keys[i]);
        } else if (x.children[i + 1].numKeys >= ORDER) {
            x.keys[i] = getSuccessor(x.children[i + 1]);
            x.values[i] = getValueForKey(x.children[i + 1], x.keys[i]);
        } else {
            mergeNodes(x, i);
            deleteRecursive(x.children[i], key);
        }
    }

    private K getPredecessor(Node<K, V> x) {
        while (!x.isLeaf) {
            x = x.children[x.numKeys];
        }
        return x.keys[x.numKeys - 1];
    }

    private K getSuccessor(Node<K, V> x) {
        while (!x.isLeaf) {
            x = x.children[0];
        }
        return x.keys[0];
    }

    private V getValueForKey(Node<K, V> x, K key) {
        int i = 0;
        while (i < x.numKeys && key.compareTo(x.keys[i]) > 0) {
            i++;
        }
        return x.values[i];
    }

    private void mergeNodes(Node<K, V> x, int i) {
        Node<K, V> child = x.children[i];
        Node<K, V> sibling = x.children[i + 1];
        child.keys[ORDER - 1] = x.keys[i];
        child.values[ORDER - 1] = x.values[i];
        for (int j = 0; j < sibling.numKeys; j++) {
            child.keys[ORDER + j] = sibling.keys[j];
            child.values[ORDER + j] = sibling.values[j];
            child.numKeys++;
        }
        if (!child.isLeaf) {
            for (int j = 0; j <= sibling.numKeys; j++) {
                child.children[ORDER + j] = sibling.children[j];
            }
        }
        for (int j = i + 1; j < x.numKeys; j++) {
            x.keys[j - 1] = x.keys[j];
            x.values[j - 1] = x.values[j];
            x.children[j] = x.children[j + 1];
        }
        x.numKeys--;
    }

    private void fillNode(Node<K, V> x, int i) {
        if (i != 0 && x.children[i - 1].numKeys >= ORDER) {
            borrowFromPrev(x, i);
        } else if (i != x.numKeys && x.children[i + 1].numKeys >= ORDER) {
            borrowFromNext(x, i);
        } else {
            if (i != x.numKeys) {
                mergeNodes(x, i);
            } else {
                mergeNodes(x, i - 1);
            }
        }
    }

    private void borrowFromPrev(Node<K, V> x, int i) {
        Node<K, V> child = x.children[i];
        Node<K, V> sibling = x.children[i - 1];
        for (int j = child.numKeys; j > 0; j--) {
            child.keys[j] = child.keys[j - 1];
            child.values[j] = child.values[j - 1];
        }
        if (!child.isLeaf) {
            for (int j = child.numKeys + 1; j > 0; j--) {
                child.children[j] = child.children[j - 1];
            }
        }
        child.keys[0] = x.keys[i - 1];
        child.values[0] = x.values[i - 1];
        if (!child.isLeaf) {
            child.children[0] = sibling.children[sibling.numKeys];
        }
        x.keys[i - 1] = sibling.keys[sibling.numKeys - 1];
        x.values[i - 1] = sibling.values[sibling.numKeys - 1];
        child.numKeys++;
        sibling.numKeys--;
    }

    private void borrowFromNext(Node<K, V> x, int i) {
        Node<K, V> child = x.children[i];
        Node<K, V> sibling = x.children[i + 1];
        child.keys[child.numKeys] = x.keys[i];
        child.values[child.numKeys] = x.values[i];
        if (!child.isLeaf) {
            child.children[child.numKeys + 1] = sibling.children[0];
        }
        x.keys[i] = sibling.keys[0];
        x.values[i] = sibling.values[0];
        for (int j = 1; j < sibling.numKeys; j++) {
            sibling.keys[j - 1] = sibling.keys[j];
            sibling.values[j - 1] = sibling.values[j];
            if (!sibling.isLeaf) {
                sibling.children[j - 1] = sibling.children[j];
            }
        }
        if (!sibling.isLeaf) {
            sibling.children[sibling.numKeys - 1] = sibling.children[sibling.numKeys];
        }
        child.numKeys++;
        sibling.numKeys--;
    }

    private void saveRoot(Node<K, V> node) throws IOException {
        lock.writeLock().lock();
        try {
            raf.seek(0);
            raf.writeBoolean(node.isLeaf);
            raf.writeInt(node.numKeys);
            for (int i = 0; i < node.numKeys; i++) {
                writeKey(node.keys[i]);
                writeValue(node.values[i]);
            }
            if (!node.isLeaf) {
                for (int i = 0; i <= node.numKeys; i++) {
                    raf.writeLong(node.children[i].pos);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private Node<K, V> loadRoot() throws IOException {
        lock.readLock().lock();
        try {
            raf.seek(0);
            boolean isLeaf = raf.readBoolean();
            int numKeys = raf.readInt();
            Node<K, V> node = new Node<>(isLeaf);
            node.numKeys = numKeys;
            for (int i = 0; i < numKeys; i++) {
                node.keys[i] = readKey();
                node.values[i] = readValue();
            }
            if (!isLeaf) {
                for (int i = 0; i <= numKeys; i++) {
                    long pos = raf.readLong();
                    node.children[i] = loadNode(pos);
                }
            }
            return node;
        } finally {
            lock.readLock().unlock();
        }
    }

    private Node<K, V> loadNode(long pos) throws IOException {
        lock.readLock().lock();
        try {
            raf.seek(pos);
            boolean isLeaf = raf.readBoolean();
            int numKeys = raf.readInt();
            Node<K, V> node = new Node<>(isLeaf);
            node.pos = pos;
            node.numKeys = numKeys;
            for (int i = 0; i < numKeys; i++) {
                node.keys[i] = readKey();
                node.values[i] = readValue();
            }
            if (!isLeaf) {
                for (int i = 0; i <= numKeys; i++) {
                    long childPos = raf.readLong();
                    node.children[i] = loadNode(childPos);
                }
            }
            return node;
        } finally {
            lock.readLock().unlock();
        }
    }

    private void writeKey(K key) throws IOException {
        byte[] keyBytes = toBytes(key);
        raf.writeInt(keyBytes.length);
        raf.write(keyBytes);
    }

    private void writeValue(V value) throws IOException {
        byte[] valueBytes = toBytes(value);
        raf.writeInt(valueBytes.length);
        raf.write(valueBytes);
    }

    @SuppressWarnings("unchecked")
    private K readKey() throws IOException {
        int keyLength = raf.readInt();
        byte[] keyBytes = new byte[keyLength];
        raf.read(keyBytes);
        return (K) fromBytes(keyBytes);
    }

    @SuppressWarnings("unchecked")
    private V readValue() throws IOException {
        int valueLength = raf.readInt();
        byte[] valueBytes = new byte[valueLength];
        raf.read(valueBytes);
        return (V) fromBytes(valueBytes);
    }

    @SuppressWarnings("unchecked")
    private byte[] toBytes(Object obj) {
        if (obj instanceof Comparable) {
            return ((Comparable<Object>) obj).toString().getBytes();
        } else {
            throw new IllegalArgumentException("Object must implement Comparable");
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T fromBytes(byte[] bytes) {
        return (T) Arrays.toString(bytes);
    }

    @Override
    public void close() throws IOException {
        raf.close();
        indexFile.delete();
    }

    private static class Node<K extends Comparable<K>, V> {
        private final boolean isLeaf;
        private int numKeys;
        private K[] keys;
        private V[] values;
        private Node<K, V>[] children;
        private long pos;

        @SuppressWarnings("unchecked")
        Node(boolean isLeaf) {
            this.isLeaf = isLeaf;
            this.keys = (K[]) new Comparable[2 * ORDER - 1];
            this.values = (V[]) new Object[2 * ORDER - 1];
            this.children = isLeaf ? null : new Node[2 * ORDER];
        }

        void splitChild(int i, Node<K, V> y) {
            Node<K, V> z = new Node<>(y.isLeaf);
            z.numKeys = ORDER - 1;
            for (int j = 0; j < ORDER - 1; j++) {
                z.keys[j] = y.keys[j + ORDER];
                z.values[j] = y.values[j + ORDER];
            }
            if (!y.isLeaf) {
                System.arraycopy(y.children, 4, z.children, 0, ORDER);
            }
            y.numKeys = ORDER - 1;
            for (int j = numKeys; j >= i + 1; j--) {
                children[j + 1] = children[j];
            }
            children[i + 1] = z;
            for (int j = numKeys - 1; j >= i; j--) {
                keys[j + 1] = keys[j];
                values[j + 1] = values[j];
            }
            keys[i] = y.keys[ORDER - 1];
            values[i] = y.values[ORDER];
        }
    }
}