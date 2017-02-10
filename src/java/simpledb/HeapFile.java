package simpledb;

import java.io.*;
import java.util.*;
import java.io.RandomAccessFile;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private HeapPage[] pages;
    private int numPages;
    private final int tableId;
    private TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tableId = f.getAbsoluteFile().hashCode();
        this.td = td;
        int pageSize = BufferPool.PAGE_SIZE;
        int pagesNeeded;

        RandomAccessFile rf;
        try {
            rf = new RandomAccessFile(f, "r");
            pagesNeeded = (int) Math.ceil((double) rf.length() / pageSize);
        } catch (Exception e) {
            System.err.println("Caugth exception1:" + e.getMessage());
            return;
        }
        this.numPages = pagesNeeded;
        System.out.println("Created " + pagesNeeded + " heap files.");
        pages = new HeapPage[pagesNeeded];

        // TODO Find out the best way to handle exceptions
        for (int pageIndex = 0; pageIndex < pagesNeeded; ++pageIndex) {
            int offset = pageIndex * pageSize;
            byte[] data = new byte[pageSize];
            try {
                rf.readFully(data, offset, pageSize);
                HeapPageId pid = new HeapPageId(tableId, pageIndex);
                System.out.println("Created HeapPageId");
                pages[pageIndex] = new HeapPage(pid, data);
                System.out.println("Wrote to pages["+pageIndex+"].");
            } catch (Exception e) {
                System.err.println("Caught exception2:" + e.getMessage());
                return;
            }
        }
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return tableId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return numPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t)
            throws DbException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        System.out.println("numPages is " + numPages);
        return new TupleIter(numPages);
    }

    private class TupleIter implements DbFileIterator {

        private int pageNum;
        private int numPages;
        private boolean open;
        private Iterator<Tuple> iter;
        public TupleIter(int numPages) {
            open = false;
            this.numPages = numPages;
        }

        public void open() {
            open = true;
            pageNum = 0;
            System.out.println("pageNum is " + pageNum);
            System.out.println("pages has length " + pages.length);
            iter = pages[pageNum].iterator();
        }

        public boolean hasNext() {
            return open && ((pageNum < numPages - 1) || iter.hasNext());
        }

        public Tuple next() {
            if (!open) {
                throw new NoSuchElementException();
            }
            if (!(iter.hasNext())) {
                if (pageNum == numPages - 1) {
                    throw new NoSuchElementException();
                }
                ++pageNum;
                iter = pages[pageNum].iterator();
            }
            return iter.next();
        }

        public void rewind() {
            open();
        }

        public void close() {
            open = false;
            pageNum = -1;
            iter = null;
        }
    }
}

