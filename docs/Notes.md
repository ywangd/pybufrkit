## Operators

### 204 YYY
* 031021 can be used multiple times inside one 204YYY session to change the
  meaning of the attached bits.
* Every new 204YYY session must be immediately followed by a 031021 field.

* How to define compound associated field (not just simply repeating 204YYY)?

### Data present bit-map
It can be defined immediately after an operator that requires it, e.g. 224000.
Otherwise, its definition requires an opening 236000. If it is to be re-used, 
it then must be defined using 236000.

* Bit 0 means data **present**, while 1 means data **NOT** present.
* Multiple Bit-map definition is possible and stored like a stack? A cancel 
  operator (237255) only cancels the most recent definition. But 235000 can 
  cancel all definitions at once.
  

* `mpco_217.bufr` is malformed
There is an erroneous replication descriptor `114000` which should be `116000` 
to include 2 more items. This file has been corrected so that the decoding can pass.
