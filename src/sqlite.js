/**
 * Module handles database management
 *
 * Server API calls the methods in here to query and update the SQLite database
 */

// Utilities we need
const fs = require("fs");

// Initialize the database
const dbFile = "./.data/choices.db";
const exists = fs.existsSync(dbFile);
const sqlite3 = require("sqlite3").verbose();
const dbWrapper = require("sqlite");

const { parse } = require("csv-parse");
let db;

/* 
We're using the sqlite wrapper so that we can make async / await connections
- https://www.npmjs.com/package/sqlite
*/

async function initializeDatabase() {
  console.log("Load athletes database");
  try {
    await db.run(
      "CREATE TABLE Athletes (id INTEGER PRIMARY KEY AUTOINCREMENT, fullname TEXT)"
    );

    await db.run(
      "CREATE TABLE Competitions (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, year INTEGER)"
    );

    await db.run(
      "CREATE TABLE Results (id INTEGER PRIMARY KEY AUTOINCREMENT, athlete INTEGER, competition INTEGER, time INTEGER, rank INTEGER)"
    );
    
    await loadCompetition("SM12", 2012, "results-2012.csv");
    await loadCompetition("SM13", 2013, "results-2013.csv");
    await loadCompetition("SM14", 2014, "results-2014.csv");
    await loadCompetition("SM15", 2015, "results-2015.csv");
    await loadCompetition("SM16", 2016, "results-2016.csv");
    await loadCompetition("SM17", 2017, "results-2017.csv");
    await loadCompetition("SM18", 2018, "results-2018.csv");
    await loadCompetition("SM19", 2019, "results-2019.csv");
    await loadCompetition("SM21", 2021, "results-2021.csv");
    await loadCompetition("SM22", 2022, "results-2022.csv");
    
    await generateGlobalRanking();
    
  } catch (dbError) {
    console.error(dbError);
  }
}

const removeAccents = (str) =>
  str.normalize("NFD").replace(/[\u0300-\u036f]/g, "");

async function loadCompetition(competitionName, year, filename) {
  try {
    const competitionId = await createCompetition(competitionName, year);
    console.log("Loading " + competitionName + ": " + competitionId);

    const parser = fs
      .createReadStream("./src/" + filename)
      .pipe(parse({ delimiter: ",", from_line: 1 }));
    for await (const row of parser) {
      var rank = row[0];
      var name = row[2];
      var surname = row[3];
      var fullname = name + " " + surname;
      var time = row[6];
      console.log("Loading " + competitionName + "-" +  rank)
      var athleteId = await createAthlete(fullname);
      await fillCompetitionResults(competitionId, athleteId, rank, time);
    }
  } catch (dbError) {
    console.error(dbError);
  }
}

async function createAthlete(fullName) {
  try {
    const normFullName = removeAccents(fullName);

    const athletes = await db.all(
      "SELECT * from Athletes WHERE fullname = ?",
      normFullName
    );
    if (athletes.length > 0) {
      //console.debug(        normFullName + " already exists, returning " + athletes[0].id      );
      return athletes[0].id;
    } else {
      //console.debug(normFullName + " is new, inserting");
      await db.run("INSERT INTO Athletes (fullname) VALUES (?)", [
        normFullName,
      ]);
      const lastId = await db.get(
        "select id from Athletes where fullname = ?",
        [normFullName]
      );
      //console.debug(normFullName + " " + lastId.id);
      return lastId.id;
    }
  } catch (dbError) {
    console.error(dbError);
  }
}

async function createCompetition(name, year) {
  try {
    console.log("Creating competition " + name + " " + year);
    await db.run("INSERT INTO Competitions (name,year) VALUES (?,?)", [
      name,
      year,
    ]);
    const lastId = await db.get(
      "select id from Competitions where name = ? and year = ?",
      [name, year]
    );
    console.log(name + "/" + year + " " + lastId.id);
    return lastId.id;
  } catch (dbError) {
    console.error(dbError);
  }
}

function convertTimeToSeconds(time) {
  var tokens = time.split(":");
  return +tokens[0] * 60 * 60 + +tokens[1] * 60 + +tokens[2];
}

function convertSecondsToTime(seconds) {
  return new Date(seconds * 1000).toISOString().substring(11, 19);
}


async function fillCompetitionResults(competition, athleteId, rank, time) {
  try {
    //console.log("Result for " + athleteId + ": " + rank + "/" + time);
    await db.run(
      "INSERT INTO Results (athlete, competition, time, rank) VALUES (?,?,?,?)",
      [athleteId, competition, convertTimeToSeconds(time), rank]
    );
  } catch (dbError) {
    console.error(dbError);
  }
}

async function generateGlobalRanking() {
  try {
    console.log("Generating global ranking");
    //TODO: Convert time literal to seconds
    await db.run(
      "create table global_ranking (position integer primary key autoincrement, athlete_id integer, athlete_fullname text, competition_id integer, competition_year integer, time integer)"
    );
    await db.run(
      "insert into global_ranking( athlete_id, athlete_fullname, competition_id, competition_year, time) select a.id,  a.fullname, c.id, c.year, min(r.time) as time from athletes a inner join results r on a.id = r.athlete inner join competitions c on c.id = r.competition group by a.fullname order by min(r.time)"
    );
  } catch (dbError) {
    console.error(dbError);
  }
}

dbWrapper
  .open({
    filename: dbFile,
    driver: sqlite3.Database,
  })
  .then(async (dBase) => {
    db = dBase;

    // We use try and catch blocks throughout to handle any database errors
    try {
      console.log("Initializing db");
      // The async / await syntax lets us write the db operations in a way that won't block the app
      if (!exists) {
        console.log("New db");
        // Database doesn't exist yet

        await initializeDatabase(db);
      } else {
        // We have a database already - write Choices records to log for info

        console.log("old  db");

        //If you need to remove a table from the database use this syntax
        //db.run("DROP TABLE Logs"); //will fail if the table doesn't exist
      }
    } catch (dbError) {
      console.error(dbError);
    }
  });

// Our server script will call these methods to connect to the db
module.exports = {

  getTopAthletes: async () => {
    try {
      var topAthletes = await db.all(
        "select * from global_ranking limit 50"
      );

      for (var i = 0; i < topAthletes.length; i++) {
        console.log(convertSecondsToTime(topAthletes[i].time));
        topAthletes[i].time = convertSecondsToTime(topAthletes[i].time);
      }
      return topAthletes;
    } catch (dbError) {
      console.error(dbError);
    }
  },
  
  searchAthleteInRanking: async (fullname) => {
    console.log(fullname)
    const param = '%' + removeAccents(fullname).toUpperCase() + '%';
    try {
      var topAthletes = await db.all(
        "select * from global_ranking where athlete_fullname like ?",
        [param]
      );

      for (var i = 0; i < topAthletes.length; i++) {
        console.log(convertSecondsToTime(topAthletes[i].time));
        topAthletes[i].time = convertSecondsToTime(topAthletes[i].time);
      }
      return topAthletes;
    } catch (dbError) {
      console.error(dbError);
    }
  },
  
  
  getCompetitionCount: async () => {
    try {
      return await db.all(
        "select c.year as year, count(*) as num  from results r inner join competitions c on r.competition = c.id  group by competition order by 1;"
      );

    } catch (dbError) {
      console.error(dbError);
    }
  },
  
  getCompetitionByYear: async (year) => {
    try {
      let result = await db.get(
        "select c.year as year, count(*) as participants , a.fullname as winner, winners.time from results r inner join competitions c on r.competition = c.id inner join results winners on c.id = winners.competition and winners.rank = 1 inner join athletes a on winners.athlete = a.id where year = ? group by r.competition order by 1;",
        [year]
      );
      result.time = convertSecondsToTime(result.time);
      return result;
    } catch (dbError) {
      console.error(dbError);
    }
  },
  
  getDistributionByYear: async (year) => {
    try {
      return await db.all(
        "select r.time/60 as minute, count(*) as num from results r inner join competitions c on c.id = r.competition where c.year = ? group by r.time/60 order by 1;",
        [year]
      );

    } catch (dbError) {
      console.error(dbError);
    }
  },
  
  getBestTimes: async () => {
    try {
      return await db.all(
        "select c.year, r.time as mark  from results r inner join competitions c on c.id = r.competition where r.rank = 1 order by 1"
      );

    } catch (dbError) {
      console.error(dbError);
    }
  },
  
    getBestTimesByAthlete: async (athlete) => {
    try {
      return await db.all(
        "with totals as (select competition,count(*) as num from results group by competition) select c.year, r.time as mark, r.rank, t.num  from results r inner join competitions c on c.id = r.competition inner join totals t on c.id = t.competition where r.athlete = ? order by 1;",
        [athlete]
      );

    } catch (dbError) {
      console.error(dbError);
    }
  },
  
  getWinners: async () => {
    try {
      const winners = await db.all(
        "select c.year, r.time as mark, a.fullname from athletes a inner join results r on a.id = r.athlete inner join competitions c on c.id = r.competition where r.rank = 1 order by 1 desc;"
      );
      for (var i = 0; i < winners.length; i++) {
        winners[i].mark = convertSecondsToTime(winners[i].mark);
      }
      return winners;

    } catch (dbError) {
      console.error(dbError);
    }
  },
  
  getRankingInfoByAthlete: async (id) => {
      console.log("rank by id " + id)
    try {
      const rankinfo = await db.all(
        "select g.* from global_ranking g inner join global_ranking me where g.position <  me.position+5 and g.position > me.position-5  and me.athlete_id = ?",
        [id]
      );
      console.log("Found " + rankinfo.length)
      for (var i = 0; i < rankinfo.length; i++) {
        rankinfo[i].time = convertSecondsToTime(rankinfo[i].time);
      }
      return rankinfo;

    } catch (dbError) {
      console.error(dbError);
    }
  },
  
  getAthleteById: async (id) => {
      console.log("athlete by id " + id)
    try {
      const rankinfo = await db.all(
        "select * from global_ranking where athlete_id = ?",
        [id]
      );
      console.log("Found " + rankinfo.length)
      for (var i = 0; i < rankinfo.length; i++) {
        rankinfo[i].time = convertSecondsToTime(rankinfo[i].time);
      }
      return rankinfo[0];

    } catch (dbError) {
      console.error(dbError);
    }
  },
  
  
  //
  
};
