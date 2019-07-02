"use strict";

const fs = require("fs");
const commander = require("commander");
const cassandra = require("cassandra-driver");
const _ = require("lodash");

const packageJson = require("./package.json");
const Shift = require("cassandra-shift");

let command;

commander
  .version(packageJson.version)
  .arguments("<cmd>")
  .command("migrate", "Migrate the schema to the latest version")
  .command("clean", "Drops all objects in the keypace")
  .command("info", "Prints the details about all the migrations")
  .command("validate", "Validates the applied migrations against the available ones")
  .option("-c", "--config", "Config file")
  .option("-n", "--number-of-clients", "Number of clients")
  .option("-p", "--contact-points", "Contact points")
  .option("-d", "--local-data-center", "Local data center")
  .option("-a", "--ssl-ca", "SSL CA")
  .option("-t", "--ssl-cert", "SSL certificate")
  .option("-y", "--ssl-key", "SSL key")
  .option("-u", "--username", "Username")
  .option("-p", "--password", "Password")
  .option("-k", "--keyspace", "Keyspace")
  .option("-e", "--ensure-keyspace", "Ensure keyspace (creates if not exists)")
  .option("-d", "--directory", "Migrations directory")
  .action((cmd) => command = cmd);

let fileConfig = {};
try {
  if (commander.config)
    fileConfig = require(commander.config);
}
catch(err) {
  console.log("Unable to load specified config file");
  process.exit(1);
}

let envConfig = {
  numberOfClients: process.env.NUMBER_OF_CLIENTS,
  cassandra: {
    contactPoints: process.env.CONTACT_POINTS,
    localDataCenter: process.env.LOCAL_DATA_CENTER,
    keyspace: process.env.KEYSPACE,
    sslOptions: {
      ca: process.env.SSL_CA,
      cert: process.env.SSL_CERT,
      key: process.env.SSL_KEY
    },
    auth: {
      username: process.env.USERNAME,
      password: process.env.PASSWORD
    }
  },
  ensureKeyspace: process.env.ENSURE_KEYSPACE,
  dir: process.env.MIGRATIONS_DIRECTORY
};

let cliConfig = {
  cassandra: {
    contactPoints: commander["number-of-clients"],
    localDataCenter: commander["local-data-center"],
    keyspace: commander.keyspace,
    sslOptions: {
      ca: commander["ssl-ca"],
      cert: commander["ssl-cert"],
      key: commander["ssl-key"]
    },
    auth: {
      username: commander.username,
      password: commander.password
    }
  },
  ensureKeyspace: commander["ensure-keyspace"],
  dir: commander.directory
};

let config = _.merge({
  numberOfClients: 1
}, fileConfig, envConfig, cliConfig);

if (config.cassandra && config.cassandra.sslOptions) {
  if (config.cassandra.sslOptions.ca)
    config.cassandra.sslOptions.ca = fs.readFileSync(config.cassandra.sslOptions.ca);
  
  if (config.cassandra.sslOptions.cert)
    config.cassandra.sslOptions.cert = fs.readFileSync(config.cassandra.sslOptions.cert);

  if (config.cassandra.sslOptions.key)
    config.cassandra.sslOptions.key = fs.readFileSync(config.cassandra.sslOptions.key);
}

if (config.cassandra && config.cassandra.auth && cassandra.auth.username && cassandra.auth.password) {
  config.cassandra = new cassandra.auth.PlainTextAuthProvider(cassandra.auth.username, cassandra.auth.password);
}

let cassandraClients;
try {
  for (let i = 0; i < config.numberOfClients; ++i)
    cassandraClients.push(new cassandra.Client(config.cassandra));
}
catch(err) {
  console.log("Error configuring Cassandra driver", err);
}

(async () => {
  let shiftOpts = _.omit(config, ["cassandra", "numberOfClients"]);

  let shift = new Shift(cassandraClients, shiftOpts);

  shift.on("ensuredKeyspace", () => {
    console.log("Ensured keyspace");
  });

  shift.on("ensuredKeyspace", () => {
    console.log("Using keyspace");
  });

  shift.on("ensuredKeyspace", () => {
    console.log("Ensured migration table");
  });

  shift.on("checkedAppliedMigrations", () => {
    console.log("Checked applied migrations");
  });

  try {
    switch(command) {
    default:
      console.log("Invalid command");
      break;
    case "migrate":
      shift.on("appliedMigration", (mm) => {
        console.log(`Applied migration ${mm.version} "${mm.name}" (${mm.type})`);
      });

      await shift.migrate();
      break;
    case "clean":
      await shift.clean();
      break;
    case "info":
      await shift.info();
      break;
    case "validate":
      await shift.validate();
      break;
    }
  }
  catch(err) {
    console.log("Error executing command", err);
    process.exit(1);
  }

  process.exit(0);
})();