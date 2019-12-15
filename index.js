const lineReader = require('line-reader');
// const NodeCache = require('node-cache');
// const cache = new NodeCache();
const Promise = require('bluebird');
const fs = require('fs');
// const cacheManager = require('cache-manager');
// const redisStore = require('cache-manager-redis-store');
// const cache = cacheManager.caching({
// 	store: redisStore,
// 	// host: 'localhost', // default value
// 	// port: 6379, // default value
// 	// auth_pass: 'XXXXX',
// 	// db: 0,
// 	// ttl: 600
// });
// const {promisify} = require('util');
const redis = require('redis');
Promise.promisifyAll(redis);
const redisClient = redis.createClient();
redisClient.on('error', function (err) {
    console.log('Redis Error ' + err);
});

// const redisHGET = promisify(redisClient.hget).bind(redisClient)

(async () => {
  // console.log('Reading IMDb ratings data...');
  let streamsEnded = 0;

  if (!await redisClient.existsAsync('imdb_ratings') || !await redisClient.existsAsync('imdb_votes')) {
    const ratingReadStream = fs.createReadStream('./title.ratings.tsv')
      // .on('end', () => ratingReadStream.destroy());

    lineReader.eachLine(ratingReadStream, (ratingLine, isLast) => {
      const rating = ratingLine.split("\t");

			redisClient.hset('imdb_ratings', rating[0], rating[1]);
			redisClient.hset('imdb_votes', rating[0], rating[2]);

      if(isLast) {
          streamsEnded++;
          return false;
      }
    });
  }
	else {
			streamsEnded++;
	}

  // console.log('Rating reading done.', cachedRatings.length)
  // console.log('Reading IMDb episode data...');

  // if (!await redisClient.existsAsync('imdb_episodes:*')) {
  //   const episodeReadStream = fs.createReadStream('./title.episode.tsv')
  //     // .on('end', () => episodeReadStream.destroy());
	//
  //   lineReader.eachLine(episodeReadStream, (episodeLine, isLast) => {
  //     const episode = episodeLine.split("\t");
	//
	// 		redisClient.rpush(`imdb_episodes:${episode[1]}`, episode[0]);
	//
  //     if(isLast) {
  //         streamsEnded++;
  //         return false;
  //     }
  //   })
  // }
  // else {
    streamsEnded++;
  // }

  // console.log('Episode reading done.', cachedEpisodes.length)
  let timePassed = 0;
	const imdbId = process.argv.slice(2)[0];

	const asyncForEach = async (array, callback) => {
	  for (let index = 0; index < array.length; index++) {
	    await callback(array[index], index, array)
	  }
	}

	const episodeRatingsInterval = setInterval(() => (async () => {
    const selectedEpisodes = await redisClient.lrangeAsync(`imdb_episodes:${imdbId}`, 0, -1) || [];
    const selectedRating = await redisClient.hgetAsync('imdb_ratings', imdbId) || 0;
		const selectedVotes = await redisClient.hgetAsync('imdb_votes', imdbId) || 0;

    let episodeAverageRating = 0;
    let episodeTotalVotes = 0;


    await asyncForEach(selectedEpisodes, async (selectedEpisodeId) => {
			const episodeRating = await redisClient.hgetAsync('imdb_ratings', selectedEpisodeId) || 0;
			episodeAverageRating += parseFloat(episodeRating);

			const episodeVotes = await redisClient.hgetAsync('imdb_votes', selectedEpisodeId) || 0;
			episodeTotalVotes += parseInt(episodeVotes);
    });

    const stats = {
      rating: parseFloat(selectedRating),
      ratingVotes: parseInt(selectedVotes),
      episodeAverageRating: (episodeAverageRating / selectedEpisodes.length).toFixed(3),
      episodeTotalVotes,
      streamsEnded,
      iterations: timePassed+=1,
    };

    process.stdout.clearLine();
    process.stdout.cursorTo(0);
    process.stdout.write(JSON.stringify(stats));

    if(streamsEnded === 2) {
      process.stdout.write("\nDone!");
      clearInterval(episodeRatingsInterval);
      process.exit();
    }
  })(), 2000);
})();
