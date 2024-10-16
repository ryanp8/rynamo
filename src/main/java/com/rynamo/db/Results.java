package com.rynamo.db;

import java.util.List;
public record Results(long version, List<byte[]> values) { }
