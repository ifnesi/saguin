# Why there is a seeded device list here

`devices.jsonl` tells zigbee2mqtt it already knows three devices, so it does not
have to interview them when the demo starts.

**That is the normal state of a hub**, not a shortcut around the demo's claim.
A hub you have been running for a year has paired devices; pairing is a
separate thing and deserves a demo of its own. What this demo shows is what a
paired installation gains from Sagüin day to day.

Everything else still comes over the radio. The devices in this file announce
themselves, report their readings and act on commands, all as frames from the
coordinator emulator, and **zigbee2mqtt is the only thing that publishes to
Sagüin**. Nothing writes device state to MQTT directly.

It is copied to `/app/data/database.db` when the container first starts -
zigbee2mqtt's own format, which is JSON Lines. The name here is `.jsonl`
because `.db` is in this repository's `.gitignore`, a rule meant for sqlite
databases rather than for a checked-in fixture.

The addresses and model names here must match `coordinator/devices.go`. The
models are real ones zigbee2mqtt has converters for, chosen because they report
over standard ZCL clusters rather than a vendor's own protocol - and because
their converters read no manufacturer-specific attributes, which an emulated
device would have to invent:

| device | model | vendor |
|---|---|---|
| living-room-sensor, bedroom-sensor | `CK-TLSR8656-SS5-02(7014)` | eWeLink |
| kitchen-plug | `LDSENK01F` | ADEO |
