package htsjdk.samtools;

import htsjdk.HtsjdkTest;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class QueryIntervalTest extends HtsjdkTest {

    @Test
    public void testOptimizeIntervals() throws Exception {
        final QueryInterval[] overlappingIntervals = new QueryInterval[]{
            new QueryInterval(0, 1520, 1521),
            new QueryInterval(0, 1521, 1525)
        };

        final QueryInterval[] optimizedOverlapping = QueryInterval.optimizeIntervals(overlappingIntervals);

        final QueryInterval[] abuttingIntervals = new QueryInterval[]{
            new QueryInterval(0, 1520, 1521),
            new QueryInterval(0, 1522, 1525)
        };

        final QueryInterval[] optimizedAbutting = QueryInterval.optimizeIntervals(abuttingIntervals);

        final QueryInterval[] expected = new QueryInterval[]{
            new QueryInterval(0, 1520, 1525),
        };

        Assert.assertEquals(optimizedOverlapping, expected);
        Assert.assertEquals(optimizedAbutting, expected);


        final QueryInterval[]
            nonOptimizableSeparatedIntervals = new QueryInterval[]{
            new QueryInterval(0, 1520, 1521),
            new QueryInterval(0, 1523, 1525)
        };

        final QueryInterval[] optimizedSeparated = QueryInterval.optimizeIntervals(nonOptimizableSeparatedIntervals);

        Assert.assertEquals(optimizedSeparated, nonOptimizableSeparatedIntervals);
    }

    @DataProvider(name = "nonOptimizedIntervalsProvider")
    public Object[][] nonOptimizedIntervalsProvider() {
        return new Object[][]{
            {new QueryInterval[]{new QueryInterval(0, 10, 20), new QueryInterval(0, 15, 25)}},
            {new QueryInterval[]{new QueryInterval(0, 10, 19), new QueryInterval(0, 20, 30)}},
            {new QueryInterval[]{new QueryInterval(0, 20, 30), new QueryInterval(0, 10, 20)}},
        };
    }

    @Test(dataProvider = "nonOptimizedIntervalsProvider", expectedExceptions = IllegalArgumentException.class)
    public void testAssertIntervalsOptimizedFailure(final QueryInterval[] intervals) {
        QueryInterval.assertIntervalsOptimized(intervals);
    }
}
