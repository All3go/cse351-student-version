using System.Diagnostics;

namespace assignment11;

public class Assignment11
{
    private const long START_NUMBER = 10_000_000_000;
    private const int RANGE_COUNT = 1_000_000;

    private static bool IsPrime(long n)
    {
        if (n <= 3) return n > 1;
        if (n % 2 == 0 || n % 3 == 0) return false;

        for (long i = 5; i * i <= n; i += 6)
        {
            if (n % i == 0 || n % (i + 2) == 0)
                return false;
        }
        return true;
    }

    public static void Main(string[] args)
    {
        int numbersProcessed = 0;
        int primeCount = 0;

        Console.WriteLine("Prime numbers found:");

        Queue<long> queue = new Queue<long>();
        for (long i = START_NUMBER; i < START_NUMBER + RANGE_COUNT; i++)
        {
            queue.Enqueue(i);
        }

        object queueLock = new object();
        object printLock = new object();
        object countLock = new object();

        var stopwatch = Stopwatch.StartNew();
        
        void Worker()
        {
            while (true)
            {
                long number;
                lock (queueLock)
                {
                    if (queue.Count == 0)
                        break;
                    number = queue.Dequeue();
                }

                bool isPrime = IsPrime(number);

                lock (countLock)
                {
                    numbersProcessed++;
                    if (isPrime)
                        primeCount++;
                }

                if (isPrime)
                {
                    lock (printLock)
                    {
                        Console.Write($"{number}, ");
                    }
                }
            }
        }

        Thread[] threads = new Thread[10];
        for (int i = 0; i < threads.Length; i++)
        {
            threads[i] = new Thread(Worker);
            threads[i].Start();
        }

        for (int i = 0; i < 10; i++)
        {
            threads[i].Join();
        }

        stopwatch.Stop();

        Console.WriteLine();
        Console.WriteLine();

        Console.WriteLine($"Numbers processed = {numbersProcessed}");
        Console.WriteLine($"Primes found      = {primeCount}");
        Console.WriteLine($"Total time        = {stopwatch.Elapsed}");        
    }
}
