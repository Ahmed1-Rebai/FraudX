// FraudX graph schema. Applied idempotently by the `neo4j-init` container.
//
// Entities are nodes; transactions are aggregated onto edges
// (Account)-[:TRANSFERRED_TO {count, total_amount, first_seen, last_seen}]->(Account)
// instead of one node per transaction, so the graph grows with the number of
// entities, not the number of events.
CREATE CONSTRAINT customer_id IF NOT EXISTS FOR (n:Customer) REQUIRE n.id IS UNIQUE;
CREATE CONSTRAINT account_id  IF NOT EXISTS FOR (n:Account)  REQUIRE n.id IS UNIQUE;
CREATE CONSTRAINT device_id   IF NOT EXISTS FOR (n:Device)   REQUIRE n.id IS UNIQUE;
CREATE CONSTRAINT ip_address  IF NOT EXISTS FOR (n:IP)       REQUIRE n.address IS UNIQUE;
CREATE CONSTRAINT location_id IF NOT EXISTS FOR (n:Location) REQUIRE n.id IS UNIQUE;
// Confirmed-fraud flags drive fraud_neighbor_count / distance_to_known_fraud.
CREATE INDEX customer_fraud IF NOT EXISTS FOR (n:Customer) ON (n.fraud_flag);
CREATE INDEX account_fraud  IF NOT EXISTS FOR (n:Account)  ON (n.fraud_flag);
