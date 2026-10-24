// FraudX MongoDB bootstrap. Runs once, on first start of an empty volume
// (mounted into /docker-entrypoint-initdb.d), as the root user.
//
// - creates a least-privilege application user (readWrite on the app db only)
// - creates collections with JSON-schema validators and query-driven indexes
//
// Natural keys are stored in _id (customer_id, account_id, ...) so no extra
// unique index is needed. Validation is "moderate": new/updated documents must
// conform, existing ones are not rejected retroactively.

const dbName = process.env.MONGO_DB || "fraudx";
const appUser = process.env.MONGO_APP_USER;
const appPassword = process.env.MONGO_APP_PASSWORD;
if (!appUser || !appPassword) {
  throw new Error("MONGO_APP_USER and MONGO_APP_PASSWORD must be set");
}

const app = db.getSiblingDB(dbName);
app.createUser({ user: appUser, pwd: appPassword, roles: [{ role: "readWrite", db: dbName }] });

const riskLevel = { enum: ["LOW", "MEDIUM", "HIGH"] };

function createCollection(name, schema) {
  app.createCollection(name, {
    validator: { $jsonSchema: schema },
    validationLevel: "moderate",
    validationAction: "error",
  });
}

// Customer profile: slowly changing, read by API and evidence retrieval.
// Arrays of devices/ips are capped by writers ($push + $slice) to keep
// documents bounded; the full relationship history lives in Neo4j.
createCollection("customers", {
  bsonType: "object",
  required: ["_id", "created_at", "updated_at"],
  properties: {
    _id: { bsonType: "string" },
    name: { bsonType: "string" },
    country: { bsonType: "string", pattern: "^[A-Z]{2}$" },
    accounts: { bsonType: "array", items: { bsonType: "string" } },
    devices: { bsonType: "array", maxItems: 50, items: { bsonType: "string" } },
    known_ips: { bsonType: "array", maxItems: 50, items: { bsonType: "string" } },
    risk_profile: {
      bsonType: "object",
      properties: {
        score: { bsonType: "int", minimum: 0, maximum: 100 },
        level: riskLevel,
        updated_at: { bsonType: "date" },
      },
    },
    stats: { bsonType: "object" },
    created_at: { bsonType: "date" },
    updated_at: { bsonType: "date" },
  },
});
app.customers.createIndex({ "risk_profile.level": 1, "risk_profile.score": -1 });

createCollection("accounts", {
  bsonType: "object",
  required: ["_id", "customer_id", "kind"],
  properties: {
    _id: { bsonType: "string" },
    customer_id: { bsonType: "string" },
    kind: { enum: ["CUSTOMER", "MERCHANT"] },
    opened_at: { bsonType: "date" },
  },
});
app.accounts.createIndex({ customer_id: 1 });

createCollection("devices", {
  bsonType: "object",
  required: ["_id", "first_seen"],
  properties: {
    _id: { bsonType: "string" },
    os: { bsonType: "string" },
    user_agent: { bsonType: "string" },
    first_seen: { bsonType: "date" },
    last_seen: { bsonType: "date" },
  },
});

// One investigation per flagged transaction: risk breakdown, the evidence
// that was retrieved, the generated explanation and the analyst outcome.
createCollection("investigations", {
  bsonType: "object",
  required: ["_id", "customer_id", "risk", "status", "created_at"],
  properties: {
    _id: { bsonType: "string", description: "transaction_id" },
    customer_id: { bsonType: "string" },
    risk: {
      bsonType: "object",
      required: ["score", "level"],
      properties: {
        score: { bsonType: "int", minimum: 0, maximum: 100 },
        level: riskLevel,
        components: { bsonType: "object" },
      },
    },
    evidence: { bsonType: "object" },
    explanation: {
      bsonType: "object",
      properties: {
        summary: { bsonType: "string" },
        reasons: { bsonType: "array", items: { bsonType: "string" } },
        generator: { bsonType: "string", description: "template | llm:<provider>/<model>" },
        created_at: { bsonType: "date" },
      },
    },
    status: { enum: ["OPEN", "CONFIRMED_FRAUD", "FALSE_POSITIVE", "DISMISSED"] },
    precedents: { bsonType: "array" },
    created_at: { bsonType: "date" },
    updated_at: { bsonType: "date" },
  },
});
app.investigations.createIndex({ status: 1, created_at: -1 });
app.investigations.createIndex({ customer_id: 1, created_at: -1 });

// Metadata for trained model versions; artifacts live on disk under models/.
createCollection("model_registry", {
  bsonType: "object",
  required: ["_id", "name", "version", "artifact_path", "metrics", "trained_at"],
  properties: {
    _id: { bsonType: "string", description: "<name>:<version>" },
    name: { bsonType: "string" },
    version: { bsonType: "string" },
    artifact_path: { bsonType: "string" },
    metrics: { bsonType: "object" },
    params: { bsonType: "object" },
    feature_names: { bsonType: "array", items: { bsonType: "string" } },
    trained_at: { bsonType: "date" },
    active: { bsonType: "bool" },
  },
});
app.model_registry.createIndex(
  { name: 1, active: 1 },
  { unique: true, partialFilterExpression: { active: true } },
);

print(`fraudx: initialised database '${dbName}' with app user '${appUser}'`);
