# CMS raw protocol snapshot

Only the existing CMS envelope/EFH decoder, timestamp validation and their small
support files are copied here. `sources.json` records original commits and exact
file hashes. No gateway, writer or paged process is compiled or installed by
cms_rtf. This keeps its source bundle independently buildable on an offline host.
The copied JSON header carries its MIT license; Kungfu's license is included.
Update this snapshot deliberately when the versioned raw protocol changes.
