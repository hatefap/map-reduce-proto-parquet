package randomnumbergenerator;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Iterator;
import java.util.Random;

public class LongGenerator implements Iterable<Long>{
    private final Long limit;
    private Long current;
    private final Random random;

    public LongGenerator(Long limit) {
        this.limit = limit + 1L;
        this.current = 0L;
        this.random = new Random();
    }

    public Iterator<Long> iterator() {
        return new Iterator<Long>() {
            public boolean hasNext() {
                return current < limit;
            }

            public Long next() {
                current += 1L;
                return random.nextLong();
            }

            public void remove() {
                throw new NotImplementedException();
            }
        };
    }
}
