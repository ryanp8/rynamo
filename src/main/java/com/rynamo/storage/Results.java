package com.rynamo.storage;

import java.util.List;

/*All values corresponding to the version*/
public record Results(long version, List<byte[]> values) { }
