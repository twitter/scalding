package com.twitter.scalding.repl

import java.io.File
import java.io.FileOutputStream
import java.util.jar.JarEntry
import java.util.jar.JarOutputStream

import scala.tools.nsc.GenericRunnerCommand
import scala.tools.nsc.MainGenericRunner
import scala.tools.nsc.interpreter.ILoop
import scala.tools.nsc.io.VirtualDirectory

import com.google.common.io.Files

/**
 * A runner for a Scala REPL providing functionality extensions specific to working with
 * Scalding.
 */
object ScaldingShell
    extends MainGenericRunner {
  /**
   * An instance of the Scala REPL the user will interact with.
   */
  private var scaldingREPL: Option[ILoop] = None

  /**
   * The main entry point for executing the REPL.
   *
   * This method is lifted from [[scala.tools.nsc.MainGenericRunner]] and modified to allow
   * for custom functionality, including determining at runtime if the REPL is running,
   * and making custom REPL colon-commands available to the user.
   *
   * @param args passed from the command line.
   * @return `true` if execution was successful, `false` otherwise.
   */
  override def process(args: Array[String]): Boolean = {
    // Process command line arguments into a settings object, and use that to start the REPL.
    val command = new GenericRunnerCommand(args.toList, (x: String) => errorFn(x))
    import command.settings
    scaldingREPL = Some(new ScaldingILoop)
    scaldingREPL.get.process(settings)
  }

  /**
   * Runs an instance of the shell.
   *
   * @param args from the command line.
   */
  def main(args: Array[String]) {
    val retVal = process(args)
    if (!retVal) {
      sys.exit(1)
    }
  }

  /**
   * Creates a jar file in a temporary directory containing the code thus far compiled by the REPL.
   *
   * @return some file for the jar created, or `None` if the REPL is not running.
   */
  private[scalding] def createReplCodeJar(): Option[File] = {
    scaldingREPL.map { repl =>
      val virtualDirectory = repl.virtualDirectory
      val tempJar = new File(Files.createTempDir(),
        "scalding-repl-session-" + System.currentTimeMillis() + ".jar")
      createJar(virtualDirectory, tempJar)
    }
  }

  /**
   * Creates a jar file from the classes contained in a virtual directory.
   *
   * @param virtualDirectory containing classes that should be added to the jar.
   * @param jarFile that will be written.
   * @return the jarFile specified and written.
   */
  private def createJar(virtualDirectory: VirtualDirectory, jarFile: File): File = {
    val jarStream = new JarOutputStream(new FileOutputStream(jarFile))
    try {
      addVirtualDirectoryToJar(virtualDirectory, "", jarStream)
    } finally {
      jarStream.close()
    }

    jarFile
  }

  /**
   * Add the contents of the specified virtual directory to a jar. This method will recursively
   * descend into subdirectories to add their contents.
   *
   * @param dir is a virtual directory whose contents should be added.
   * @param entryPath for classes found in the virtual directory.
   * @param jarStream for writing the jar file.
   */
  private def addVirtualDirectoryToJar(
      dir: VirtualDirectory,
      entryPath: String,
      jarStream: JarOutputStream) {
    dir.foreach { file =>
      if (file.isDirectory) {
        // Recursively descend into subdirectories, adjusting the package name as we do.
        val dirPath = entryPath + file.name + "/"
        val entry: JarEntry = new JarEntry(dirPath)
        jarStream.putNextEntry(entry)
        jarStream.closeEntry()
        addVirtualDirectoryToJar(file.asInstanceOf[VirtualDirectory], dirPath, jarStream)
      } else if (file.hasExtension("class")) {
        // Add class files as an entry in the jar file and write the class to the jar.
        val entry: JarEntry = new JarEntry(entryPath + file.name)
        jarStream.putNextEntry(entry)
        jarStream.write(file.toByteArray)
        jarStream.closeEntry()
      }
    }
  }
}
