#!/usr/bin/python
import os
import sys
import tarfile
import optparse
import re

def get_configured_fsca_version():
    pattern = re.compile("project.version=(.+)") 
    properties_file = open("project/build.properties")
    for l in properties_file:
        m = pattern.match(l)
        if m:
            return m.groups()[0]
    raise Exception("Failed to find project version")


if __name__ == "__main__":
    parser = optparse.OptionParser()
    (options, args) = parser.parse_args()

    version = get_configured_fsca_version()
    
    release_tar = tarfile.open("fs-ca-%s.tgz" % version, mode="w:gz")
    release_tar.add("target/scala_2.9.0/fs-ca_2.9.0-%s.jar" %version, "fs-ca/fs-ca-%s.jar" % version)
    release_tar.add("README", "fs-ca/README")
    release_tar.add("LICENSE", "fs-ca/LICENSE")
    release_tar.add("CHANGES.txt", "fs-ca/CHANGES.txt")
    release_tar.add("conf/log4j.xml", "fs-ca/conf/log4j.xml")
    release_tar.add("conf//log4j_debug.xml", "fs-ca/conf/log4j_debug.xml")
    release_tar.add("src/main/other/fs-ca", "fs-ca/bin/fs-ca")
    release_tar.add("src/main/other/fs-ca-rehash", "fs-ca/bin/fs-ca-rehash")
    release_tar.add("project/boot/scala-2.9.0/lib/scala-library.jar", "fs-ca/lib/scala-library.jar")
    release_tar.add("lib_managed/scala_2.9.0/compile/argot_2.9.0-0.3.1.jar", "fs-ca/lib/argot-0.3.1.jar")
    release_tar.add("lib_managed/scala_2.9.0/compile/grizzled-scala_2.9.0-1.0.6.jar", "fs-ca/lib/grizzled-1.0.6.jar")
    release_tar.add("lib_managed/scala_2.9.0/compile/commons-collections-3.1.jar", "fs-ca/lib/commons-collections-3.1.jar")
    release_tar.add("lib_managed/scala_2.9.0/compile/gson-1.7.1.jar", "fs-ca/lib/gson-1.7.1.jar")
    release_tar.add("lib_managed/scala_2.9.0/compile/guava-13.0.1.jar", "fs-ca/lib/guava-13.0.1.jar")
    release_tar.add("lib_managed/scala_2.9.0/compile/log4j-1.2.16.jar", "fs-ca/lib/log4j-1.2.16.jar")
    release_tar.add("lib/stream-2.3.0-SNAPSHOT.jar", "fs-ca/lib/stream-2.3.0-SNAPSHOT.jar")
    release_tar.add("lib/commons-math3-3.0.jar", "fs-ca/lib/commons-math3-3.0.jar")
    release_tar.add("lib/uncommons-maths-1.2.2.jar", "fs-ca/lib/uncommons-maths-1.2.2.jar")
    release_tar.close()
