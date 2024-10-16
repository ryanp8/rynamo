package com.rynamo.db;

import java.util.List;
public record Row(long version, List<byte[]> values) { }
