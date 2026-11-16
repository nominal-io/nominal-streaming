# Examples

Complete programs to copy and adapt. Each reads your API key and base URL from the profile that [API Keys Setup](../api-keys-setup.md) saves, and needs a dataset RID, such as the one the [quickstart](../index.md) prints.

- [Stream a sensor loop](sensor-loop.md): read two sensors at 10 Hz, with a fallback file and logging. Rust and Python.
- [Stream from several threads](threads.md): one stream shared by a thread per motor. Rust.
- [Stream many units to one dataset](many-units.md): test units in turn on one tester, tagging each unit's points. Rust and Python.
