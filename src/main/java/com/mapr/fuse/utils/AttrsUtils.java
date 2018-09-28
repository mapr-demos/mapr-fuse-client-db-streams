package com.mapr.fuse.utils;

import com.mapr.streams.StreamDescriptor;
import org.slf4j.Logger;
import ru.serce.jnrfuse.struct.FileStat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipalNotFoundException;
import java.util.Formatter;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import static ru.serce.jnrfuse.struct.FileStat.*;

public class AttrsUtils {

    private static final Logger log = org.slf4j.LoggerFactory.getLogger(AttrsUtils.class);

    public static void setupAttrs(Path path, FileStat stat) throws IOException {
        log.info("setupAttrs for {}", path);

        setupBasicAttrs(path, stat);

        stat.st_uid.set((Number) Files.getAttribute(path, "unix:uid"));
        stat.st_gid.set((Number) Files.getAttribute(path, "unix:gid"));
        stat.st_mode.set((Number) Files.getAttribute(path, "unix:mode"));
        stat.st_nlink.set((Number) Files.getAttribute(path, "unix:nlink"));
        stat.st_size.set((Number) Files.getAttribute(path, "unix:size"));

        log.info("finished setupAttrs for {}", path);
    }

    public static void setupAttrsStream(StreamDescriptor stream, int topicAmount, Path path, FileStat stat) throws IOException {
        log.info("setupAttrs for {}", path);

        setupBasicAttrs(path.getParent(), stat);
        setupPermissionsFromStream(stream, stat);

        stat.st_size.set(topicAmount);
        stat.st_nlink.set(topicAmount + 2);
        stat.st_mode.set(S_IFDIR + S_IRUSR | 384);

        log.info("finished setupAttrs for {}", path);
    }

    public static void setupAttrsTopic(StreamDescriptor stream, int partitionAmount, Path path, FileStat stat) throws IOException {
        log.info("setupAttrs for {}", path);

        setupBasicAttrs(path.getParent().getParent(), stat);
        setupPermissionsFromStream(stream, stat);

        stat.st_size.set(partitionAmount);
        stat.st_nlink.set(partitionAmount + 2);
        stat.st_mode.set(S_IFDIR + S_IRUSR | 384);

        log.info("finished setupAttrs for {}", path);
    }

    public static void setupAttrsPartition(StreamDescriptor stream, int size, Path path, FileStat stat) throws IOException {
        log.info("setupAttrs for {}", path);

        setupBasicAttrs(path.getParent().getParent().getParent(), stat);
        setupPermissionsFromStream(stream, stat);

        stat.st_size.set(size);
        stat.st_nlink.set(1);
        stat.st_mode.set(S_IFREG + S_IRUSR | 384);

        log.info("finished setupAttrs for {}", path);
    }

    public static String attributeToString(FileStat stat) {
        if (stat == null) {
            return "NULL";
        } else {
            Formatter buffer = new Formatter();
            if (stat.st_atim == null) {
                buffer.format("<null>, ");
            } else {
                buffer.format("%.3f, ", stat.st_atim.tv_sec.doubleValue());
            }
            if (stat.st_ctim == null) {
                buffer.format("<null>, ");
            } else {
                buffer.format("%.3f, ", stat.st_ctim.tv_sec.doubleValue());
            }
            if (stat.st_mtim == null) {
                buffer.format("<null>, ");
            } else {
                buffer.format("%.3f, ", stat.st_mtim.tv_sec.doubleValue());
            }
            if (stat.st_mode == null) {
                buffer.format("<null>, ");
            } else {
                buffer.format("%x, ", stat.st_mode.longValue());
            }
            if (stat.st_flags == null) {
                buffer.format("<null>");
            } else {
                buffer.format("%x", stat.st_flags.longValue());
            }

            return buffer.toString();
        }
    }

    public static Set<PosixFilePermission> decodeMode(long mode) {
        StringBuilder s = new StringBuilder();
        decodeOctalDigit(s, mode >> 6);
        decodeOctalDigit(s, mode >> 3);
        decodeOctalDigit(s, mode);
        return PosixFilePermissions.fromString(s.toString());
    }

    public static void setUidAndGid(Path path, long uid, long gid) throws IOException {
        Files.setAttribute(path, "unix:uid", Long.valueOf(uid).intValue());
        Files.setAttribute(path, "unix:gid", Long.valueOf(gid).intValue());
    }

    private static void decodeOctalDigit(StringBuilder s, long triple) {
        s.append((triple & 4) != 0 ? 'r' : '-');
        s.append((triple & 2) != 0 ? 'w' : '-');
        s.append((triple & 1) != 0 ? 'x' : '-');
    }

    private static void setupPermissionsFromStream(StreamDescriptor stream, FileStat stat) {
        String userName = stream.getAdminPerms().substring(2);

        //Set uid to 5000 if user wasn't found
        try {
            stat.st_uid.set(UserUtils.getUid(userName));
        } catch (UserPrincipalNotFoundException e) {
            log.info("User " + userName + " wasn't found, set uid to 5000");
            stat.st_uid.set(5000);
        }

        //Set gid to 5000 if user wasn't found
        try {
            stat.st_gid.set(UserUtils.getGid(userName));
        } catch (UserPrincipalNotFoundException e) {
            log.info("User " + userName + " wasn't found, set gid to 5000");
            stat.st_gid.set(5000);
        }
    }

    private static void setupBasicAttrs(Path path, FileStat stat) throws IOException {
        BasicFileAttributes basicFileAttributes = Files.readAttributes(path, BasicFileAttributes.class, NOFOLLOW_LINKS);

        stat.st_atim.tv_sec.set(basicFileAttributes.lastAccessTime().to(TimeUnit.SECONDS));
        stat.st_ctim.tv_sec.set(basicFileAttributes.creationTime().to(TimeUnit.SECONDS));
        stat.st_mtim.tv_sec.set(basicFileAttributes.lastModifiedTime().to(TimeUnit.SECONDS));
    }

}
