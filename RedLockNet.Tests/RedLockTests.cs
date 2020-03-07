using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using log4net;
using NUnit.Framework;
using RedLockNet.SERedis;
using RedLockNet.SERedis.Configuration;
using RedLockNet.SERedis.Util;
using StackExchange.Redis;

namespace RedLockNet.Tests
{
	[TestFixture]
	public class RedLockTests
	{
		private readonly ILog logger = LogManager.GetLogger(typeof(RedLockTests));

		[OneTimeSetUp]
		public void OneTimeSetUp()
		{
			ThreadPool.SetMinThreads(100, 100);
		}

		// make sure redis is running on these
		private static readonly EndPoint ActiveServer1 = new DnsEndPoint("localhost", 6379);
		private static readonly EndPoint ActiveServer2 = new DnsEndPoint("localhost", 6380);
		private static readonly EndPoint ActiveServer3 = new DnsEndPoint("localhost", 6381);

		// make sure redis isn't running on these
		private static readonly EndPoint InactiveServer1 = new DnsEndPoint("localhost", 63790);
		private static readonly EndPoint InactiveServer2 = new DnsEndPoint("localhost", 63791);
		private static readonly EndPoint InactiveServer3 = new DnsEndPoint("localhost", 63791);

		// make sure redis is running here with the specified password
		private static readonly RedLockEndPoint PasswordedServer = new RedLockEndPoint
		{
			EndPoint = new DnsEndPoint("localhost", 6382),
			Password = "password"
		};

		private static readonly RedLockEndPoint NonDefaultDatabaseServer = new RedLockEndPoint
		{
			EndPoint = ActiveServer1,
			RedisDatabase = 1
		};

		private static readonly RedLockEndPoint NonDefaultRedisKeyFormatServer = new RedLockEndPoint
		{
			EndPoint = ActiveServer1,
			RedisKeyFormat = "{0}-redislock"
		};

		private static readonly IList<RedLockEndPoint> AllActiveEndPoints = new List<RedLockEndPoint>
		{
			ActiveServer1,
			ActiveServer2,
			ActiveServer3
		};

		private static readonly IList<RedLockEndPoint> AllInactiveEndPoints = new List<RedLockEndPoint>
		{
			InactiveServer1,
			InactiveServer2,
			InactiveServer3
		};

		private static readonly IList<RedLockEndPoint> SomeActiveEndPointsWithQuorum = new List<RedLockEndPoint>
		{
			ActiveServer1,
			ActiveServer2,
			ActiveServer3,
			InactiveServer1,
			InactiveServer2
		};

		private static readonly IList<RedLockEndPoint> SomeActiveEndPointsWithNoQuorum = new List<RedLockEndPoint>
		{
			ActiveServer1,
			ActiveServer2,
			ActiveServer3,
			InactiveServer1,
			InactiveServer2,
			InactiveServer3
		};


		[Test]
		public void TestSingleLock()
		{
			CheckSingleRedisLock(
				() => RedLockFactory.Create(SomeActiveEndPointsWithQuorum),
				RedLockStatus.Acquired);
		}

		[Test]
		public void TestOverlappingLocks()
		{
			using (var redisLockFactory = RedLockFactory.Create(AllActiveEndPoints))
			{
				var resource = $"testredislock:{Guid.NewGuid()}";

				using (var firstLock = redisLockFactory.CreateLock(resource, TimeSpan.FromSeconds(30)))
				{
					Assert.That(firstLock.IsAcquired, Is.True);

					using (var secondLock = redisLockFactory.CreateLock(resource, TimeSpan.FromSeconds(30)))
					{
						Assert.That(secondLock.IsAcquired, Is.False);
						Assert.That(secondLock.Status, Is.EqualTo(RedLockStatus.Conflicted));
					}
				}
			}
		}

		[Test]
		public async Task TestOverlappingLocksAsync()
		{
			var task = DoOverlappingLocksAsync();

			logger.Info("======================================================");

			await task;
		}

		private async Task DoOverlappingLocksAsync()
		{
			using (var redisLockFactory = RedLockFactory.Create(AllActiveEndPoints))
			{
				var resource = $"testredislock:{Guid.NewGuid()}";

				using (var firstLock = await redisLockFactory.CreateLockAsync(resource, TimeSpan.FromSeconds(30)))
				{
					Assert.That(firstLock.IsAcquired, Is.True);

					using (var secondLock = await redisLockFactory.CreateLockAsync(resource, TimeSpan.FromSeconds(30)))
					{
						Assert.That(secondLock.IsAcquired, Is.False);
						Assert.That(secondLock.Status, Is.EqualTo(RedLockStatus.Conflicted));
					}
				}
			}
		}

		[Test]
		public void TestBlockingConcurrentLocks()
		{
			var locksAcquired = 0;
			
			using (var redisLockFactory = RedLockFactory.Create(AllActiveEndPoints))
			{
				var resource = $"testblockingconcurrentlocks:{Guid.NewGuid()}";

				var threads = new List<Thread>();

				for (var i = 0; i < 2; i++)
				{
					var thread = new Thread(() =>
					{
						// ReSharper disable once AccessToDisposedClosure (we join on threads before disposing)
						using (var redisLock = redisLockFactory.CreateLock(
							resource,
							TimeSpan.FromSeconds(2),
							TimeSpan.FromSeconds(10),
							TimeSpan.FromSeconds(0.5)))
						{
							logger.Info("Entering lock");
							if (redisLock.IsAcquired)
							{
								Interlocked.Increment(ref locksAcquired);
							}
							Thread.Sleep(4000);
							logger.Info("Leaving lock");
						}
					});

					thread.Start();

					threads.Add(thread);
				}

				foreach (var thread in threads)
				{
					thread.Join();
				}
			}

			Assert.That(locksAcquired, Is.EqualTo(2));
		}

		[Test]
		public void TestSequentialLocks()
		{
			using (var redisLockFactory = RedLockFactory.Create(AllActiveEndPoints))
			{
				var resource = $"testredislock:{Guid.NewGuid()}";

				using (var firstLock = redisLockFactory.CreateLock(resource, TimeSpan.FromSeconds(30)))
				{
					Assert.That(firstLock.IsAcquired, Is.True);
				}

				using (var secondLock = redisLockFactory.CreateLock(resource, TimeSpan.FromSeconds(30)))
				{
					Assert.That(secondLock.IsAcquired, Is.True);
				}
			}
		}

		[Test]
		public void TestRenewing()
		{
			using (var redisLockFactory = RedLockFactory.Create(AllActiveEndPoints))
			{
				var resource = $"testrenewinglock:{Guid.NewGuid()}";

				int extendCount;

				using (var redisLock = redisLockFactory.CreateLock(resource, TimeSpan.FromSeconds(2)))
				{
					Assert.That(redisLock.IsAcquired, Is.True);

					Thread.Sleep(4000);

					extendCount = redisLock.ExtendCount;
				}

				Assert.That(extendCount, Is.GreaterThan(2));
			}
		}

		[Test]
		public void TestLockReleasedAfterTimeout()
		{
			using (var lockFactory = RedLockFactory.Create(AllActiveEndPoints))
			{
				var resource = $"testrenewinglock:{Guid.NewGuid()}";

				using (var firstLock = lockFactory.CreateLock(resource, TimeSpan.FromSeconds(1)))
				{
					Assert.That(firstLock.IsAcquired, Is.True);

					Thread.Sleep(550); // should cause keep alive timer to fire once
					((RedLock)firstLock).StopKeepAliveTimer(); // stop the keep alive timer to simulate process crash
					Thread.Sleep(1200); // wait until the key expires from redis

					using (var secondLock = lockFactory.CreateLock(resource, TimeSpan.FromSeconds(1)))
					{
						Assert.That(secondLock.IsAcquired, Is.True); // Eventually the outer lock should timeout
					}
				}
			}
		}

		[Test]
		public void TestQuorum()
		{
			logger.Info("======== Testing quorum with all active endpoints ========");
			CheckSingleRedisLock(
				() => RedLockFactory.Create(AllActiveEndPoints),
				RedLockStatus.Acquired);
			logger.Info("======== Testing quorum with no active endpoints ========");
			CheckSingleRedisLock(
				() => RedLockFactory.Create(AllInactiveEndPoints),
				RedLockStatus.NoQuorum);
			logger.Info("======== Testing quorum with enough active endpoints ========");
			CheckSingleRedisLock(
				() => RedLockFactory.Create(SomeActiveEndPointsWithQuorum),
				RedLockStatus.Acquired);
			logger.Info("======== Testing quorum with not enough active endpoints ========");
			CheckSingleRedisLock(
				() => RedLockFactory.Create(SomeActiveEndPointsWithNoQuorum),
				RedLockStatus.NoQuorum);
		}

		[Test]
		public void TestRaceForQuorumMultiple()
		{
			for (var i = 0; i < 2; i++)
			{
				logger.Info($"======== Start test {i} ========");

				TestRaceForQuorum();
			}
		}

		[Test]
		public void TestRaceForQuorum()
		{
			var locksAcquired = 0;

			var lockKey = $"testredislock:{ThreadSafeRandom.Next(10000)}";

			var tasks = new List<Task>();

			for (var i = 0; i < 3; i++)
			{
				var task = new Task(() =>
				{
					logger.Debug("Starting task");

					using (var redisLockFactory = RedLockFactory.Create(AllActiveEndPoints))
					{
						var sw = Stopwatch.StartNew();

						using (var redisLock = redisLockFactory.CreateLock(lockKey, TimeSpan.FromSeconds(30)))
						{
							sw.Stop();

							logger.Debug($"Lock method took {sw.ElapsedMilliseconds}ms to return, IsAcquired = {redisLock.IsAcquired}");

							if (redisLock.IsAcquired)
							{
								logger.Debug($"Got lock with id {redisLock.LockId}, sleeping for a bit");

								Interlocked.Increment(ref locksAcquired);

								// Sleep for long enough for the other threads to give up
								//Thread.Sleep(TimeSpan.FromSeconds(2));
								Task.Delay(TimeSpan.FromSeconds(2)).Wait();

								logger.Debug($"Lock with id {redisLock.LockId} done sleeping");
							}
							else
							{
								logger.Debug("Couldn't get lock, giving up");
							}
						}
					}
				}, TaskCreationOptions.LongRunning);

				tasks.Add(task);
			}

			foreach (var task in tasks)
			{
				task.Start();
			}

			Task.WaitAll(tasks.ToArray());

			Assert.That(locksAcquired, Is.EqualTo(1));
		}

		[Test]
		public void TestPasswordConnection()
		{
			CheckSingleRedisLock(
				() => RedLockFactory.Create(new List<RedLockEndPoint> { PasswordedServer }),
				RedLockStatus.Acquired);
		}

		[Test]
		[Ignore("Requires a redis server that supports SSL")]
		public void TestSslConnection()
		{
			var endPoint = new RedLockEndPoint
			{
				EndPoint = new DnsEndPoint("localhost", 6383),
				Ssl = true
			};

			CheckSingleRedisLock(
				() => RedLockFactory.Create(new List<RedLockEndPoint> { endPoint }),
				RedLockStatus.Acquired);
		}

		[Test]
		public void TestNonDefaultRedisDatabases()
		{
			CheckSingleRedisLock(
				() => RedLockFactory.Create(new List<RedLockEndPoint> { NonDefaultDatabaseServer }),
				RedLockStatus.Acquired);
		}

		[Test]
		public void TestNonDefaultRedisKeyFormat()
		{
			CheckSingleRedisLock(
				() => RedLockFactory.Create(new List<RedLockEndPoint> {NonDefaultRedisKeyFormatServer}),
				RedLockStatus.Acquired);
		}

		private static void CheckSingleRedisLock([InstantHandle]Func<RedLockFactory> factoryBuilder, RedLockStatus expectedStatus)
		{
			using (var redisLockFactory = factoryBuilder())
			{
				var resource = $"testredislock:{Guid.NewGuid()}";

				using (var redisLock = redisLockFactory.CreateLock(resource, TimeSpan.FromSeconds(30)))
				{
					Assert.That(redisLock.IsAcquired, Is.EqualTo(expectedStatus == RedLockStatus.Acquired));
					Assert.That(redisLock.Status, Is.EqualTo(expectedStatus));
				}
			}
		}
		
		[Test]
		public void TestCancelBlockingLock()
		{
			var cts = new CancellationTokenSource();

			var resource = $"testredislock:{Guid.NewGuid()}";

			using (var redisLockFactory = RedLockFactory.Create(AllActiveEndPoints))
			{
				using (var firstLock = redisLockFactory.CreateLock(
					resource,
					TimeSpan.FromSeconds(30),
					TimeSpan.FromSeconds(10),
					TimeSpan.FromSeconds(1)))
				{
					Assert.That(firstLock.IsAcquired);

					cts.CancelAfter(TimeSpan.FromSeconds(2));

					Assert.Throws<OperationCanceledException>(() =>
					{
						using (var secondLock = redisLockFactory.CreateLock(
							resource,
							TimeSpan.FromSeconds(30),
							TimeSpan.FromSeconds(10),
							TimeSpan.FromSeconds(1),
							cts.Token))
						{
							// should never get here
							Assert.Fail();
						}
					});
				}
			}
		}

		[Test]
		public void TestFactoryHasAtLeastOneEndPoint()
		{
			Assert.Throws<ArgumentException>(() =>
			{
				using (var redisLockFactory = RedLockFactory.Create(new List<RedLockEndPoint>()))
				{
				}
			});

			Assert.Throws<ArgumentException>(() =>
			{
				using (var redisLockFactory = RedLockFactory.Create((IList<RedLockEndPoint>) null))
				{
				}
			});
		}

		[Test]
		public void TestExistingMultiplexers()
		{
			using (var connectionMultiplexer = ConnectionMultiplexer.Connect(new ConfigurationOptions
			{
				AbortOnConnectFail = false,
				EndPoints = {ActiveServer1}
			}))
			{
				CheckSingleRedisLock(
					() => RedLockFactory.Create(new List<RedLockMultiplexer> {connectionMultiplexer}),
					RedLockStatus.Acquired);
			}
		}

		[Test]
		[Ignore("Timing test")]
		public async Task TimeLock()
		{
			using (var redisLockFactory = RedLockFactory.Create(AllActiveEndPoints))
			{
				var resource = $"testredislock:{Guid.NewGuid()}";

				// warmup
				for (var i = 0; i < 10; i++)
				{
					using (await redisLockFactory.CreateLockAsync(resource, TimeSpan.FromSeconds(30)))
					{
					}
				}

				var sw = new Stopwatch();
				var totalAcquire = new TimeSpan();
				var totalRelease = new TimeSpan();
				var iterations = 10000;

				for (var i = 0; i < iterations; i++)
				{
					sw.Restart();

					using (var redisLock = await redisLockFactory.CreateLockAsync(resource, TimeSpan.FromSeconds(30)))
					{
						sw.Stop();

						Assert.That(redisLock.IsAcquired, Is.True);

						logger.Info($"Acquire {i} took {sw.ElapsedTicks} ticks, status: {redisLock.Status}");
						totalAcquire += sw.Elapsed;

						sw.Restart();
					}

					sw.Stop();

					logger.Info($"Release {i} took {sw.ElapsedTicks} ticks, success");
					totalRelease += sw.Elapsed;
				}

				logger.Warn($"{iterations} iterations, total acquire time: {totalAcquire}, total release time {totalRelease}");
			}
		}
	}
}
