/**
 * This is the main server script that provides the API endpoints
 * The script uses the database helper in /src
 * The endpoints retrieve, update, and return data to the page handlebars files
 *
 * The API returns the front-end UI handlebars pages, or
 * Raw json if the client requests it with a query parameter ?raw=json
 */

// Utilities we need
const fs = require("fs");
const path = require("path");

// Require the fastify framework and instantiate it
const fastify = require("fastify")({
  // Set this to true for detailed logging:
  logger: false,
});

// Setup our static files
fastify.register(require("@fastify/static"), {
  root: path.join(__dirname, "public"),
  prefix: "/", // optional: default '/'
});

// Formbody lets us parse incoming forms
fastify.register(require("@fastify/formbody"));

const handlebars = require("handlebars");

// View is a templating manager for fastify
fastify.register(require("@fastify/view"), {
  engine: {
    handlebars,
  },
});

// Load and parse SEO data
const seo = require("./src/seo.json");
if (seo.url === "glitch-default") {
  seo.url = `https://${process.env.PROJECT_DOMAIN}.glitch.me`;
}

// We use a module for handling database operations in /src
const data = require("./src/data.json");
const db = require("./src/" + data.database);

fastify.get("/", async (request, reply) => {
    let params = request.query.raw ? {} : { seo: seo };

  // Get the log history from the db
  params.topAthletes = await db.getTopAthletes();

  // Let the user know if there's an error
  params.error = params.topAthletes ? null : data.errorMessage;
  params.pageCount = 3;
  // Send the athletes list
  return request.query.raw
    ? reply.send(params)
    : reply.view("/src/pages/index.hbs", params);
});

fastify.post("/search", async (request, reply) => {
  let params = request.query.raw ? {} : { seo: seo };

  /* 
  Authenticate the user request by checking against the env key variable
  - make sure we have a key in the env and body, and that they match
  */

  // We have a valid key and can clear the log
  console.log(request.body.fullname);
  params.fullname = request.body.fullname;
  params.topAthletes = await db.searchAthleteInRanking(params.fullname);
  const status = 200;
  // Send an unauthorized status code if the user credentials failed
  return request.query.raw
    ? reply.status(status).send(params)
    : reply.status(status).view("/src/pages/index.hbs", params);
});


fastify.get("/competitions", async (request, reply) => {
  // We only send seo if the client is requesting the front-end ui
  let params = request.query.raw ? {} : { seo: seo };

  // Flag to indicate we want to show the poll results instead of the poll form
  params.results = true;
  let options;

  options = await db.getCompetitionCount();
  if (options) {
    // We send the choices and numbers in parallel arrays
    params.optionNames = options.map((choice) => choice.year);
    params.optionCounts = options.map((choice) => choice.num);
  }
  params.error = options ? null : data.errorMessage;

  let distrib;

  distrib = await db.getDistributionByYear(2022);
  if (distrib) {
    // We send the choices and numbers in parallel arrays
    params.distribNames = distrib.map((choice) => choice.minute);
    params.distribCounts = distrib.map((choice) => choice.num);
  }
  params.error = options ? null : data.errorMessage;

  let times;

  times = await db.getBestTimes();
  if (times) {
    // We send the choices and numbers in parallel arrays
    params.timeYear = times.map((choice) => choice.year);
    params.timeMark = times.map((choice) => choice.mark);
    console.log(params.timeMark);
  }
  params.error = options ? null : data.errorMessage;

  let winners;

  winners = await db.getWinners();
  if (times) {
    // We send the choices and numbers in parallel arrays
    params.winners = winners;
  }
  params.error = options ? null : data.errorMessage;

  // Return the info to the client
  return request.query.raw
    ? reply.send(params)
    : reply.view("/src/pages/competitions.hbs", params);
});

fastify.get("/athlete", async (request, reply) => {
  let params = request.query.raw ? {} : { seo: seo };

  /* 
  Authenticate the user request by checking against the env key variable
  - make sure we have a key in the env and body, and that they match
  */
  let athleteInfo;

  athleteInfo = await db.getAthleteById(request.query.athlete);
  if (athleteInfo) {
    // We send the choices and numbers in parallel arrays
    params.athleteInfo = athleteInfo;
  }

  let times;

  times = await db.getBestTimesByAthlete(request.query.athlete);
  
  if (times) {
    // We send the choices and numbers in parallel arrays
    params.timeYear = times.map((choice) => choice.year);
    params.timeMark = times.map((choice) => choice.mark);
    params.timeRank = times.map((choice) => choice.rank);
    params.timePercentil = times.map((choice) => choice.rank*100/choice.num);
    console.log(params.timeMark);
  }

  let rankinfo;

  rankinfo = await db.getRankingInfoByAthlete(request.query.athlete);
  if (rankinfo) {
    // We send the choices and numbers in parallel arrays
    params.rankinfo = rankinfo;
  }

  const status = 200;
  // Send an unauthorized status code if the user credentials failed
  return request.query.raw
    ? reply.status(status).send(params)
    : reply.status(status).view("/src/pages/athlete.hbs", params);
});

fastify.get("/competition", async (request, reply) => {
  // We only send seo if the client is requesting the front-end ui
  let params = request.query.raw ? {} : { seo: seo };

  let year = request.query.year
  
  // Flag to indicate we want to show the poll results instead of the poll form
  params.results = true;
  let competitionDetails;

  competitionDetails = await db.getCompetitionByYear(year)
  if (competitionDetails) {
    params.competitionDetails = competitionDetails
  }
  params.error = competitionDetails ? null : data.errorMessage;

  let distrib;

  distrib = await db.getDistributionByYear(year);
  if (distrib) {
    // We send the choices and numbers in parallel arrays
    params.distribNames = distrib.map((choice) => choice.minute);
    params.distribCounts = distrib.map((choice) => choice.num);
  }
  params.error = distrib ? null : data.errorMessage;


  // Return the info to the client
  return request.query.raw
    ? reply.send(params)
    : reply.view("/src/pages/competition.hbs", params);
});



// Run the server and report out to the logs
fastify.listen(
  { port: process.env.PORT, host: "0.0.0.0" },
  function (err, address) {
    if (err) {
      fastify.log.error(err);
      process.exit(1);
    }
    console.log(`Your app is listening on ${address}`);
    fastify.log.info(`server listening on ${address}`);
  }
);
