package com.mapr.fuse;

import org.mockito.ArgumentMatcher;
import org.slf4j.Logger;

import java.nio.file.Path;

/**
 * Handy for mocking path name argument matching
 */
public class PathMatcher implements ArgumentMatcher<Path> {
  private static final Logger log = org.slf4j.LoggerFactory.getLogger(PathMatcher.class);

  private String name;

  public PathMatcher(Path root) {
    assert root != null;
    this.name = root.toString();
  }

  public boolean matches(Path name) {
    return this.name.equals(name.toString());
  }
}
