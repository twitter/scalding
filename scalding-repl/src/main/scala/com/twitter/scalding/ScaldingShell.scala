/*  Copyright 2013 Twiter, inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.twitter.scalding

import java.io.File
import java.io.FileOutputStream
import java.util.jar.JarEntry
import java.util.jar.JarOutputStream

import org.apache.hadoop.util.GenericOptionsParser
import org.apache.hadoop.conf.Configuration

import scala.tools.nsc.{ Settings, GenericRunnerCommand, MainGenericRunner }
import scala.tools.nsc.interpreter.ILoop
import scala.tools.nsc.io.VirtualDirectory

import com.google.common.io.Files

/**
 * A runner for a Scala REPL providing functionality extensions specific to working with
 * Scalding.
 */
object ScaldingShell extends MainGenericRunner {

  /** Customizable prompt. */
  var prompt: () => String = { () => Console.BLUE + "\nscalding> " + Console.RESET }

  /**
   * An instance of the Scala REPL the user will interact with.
   */
  private var scaldingREPL: Option[ILoop] = None

  /**
   * An instance of the default configuration for the REPL
   */
  private val conf: Configuration = new Configuration()

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
    // Get the mode (hdfs or local), and initialize the configuration
    val (mode, jobArgs) = parseModeArgs(args)

    // Process command line arguments into a settings object, and use that to start the REPL.
    // We ignore params we don't care about - hence error function is empty
    val command = new GenericRunnerCommand(jobArgs.toList, _ => ())

    // inherit defaults for embedded interpretter (needed for running with SBT)
    // (TypedPipe chosen arbitrarily, just needs to be something representative)
    command.settings.embeddedDefaults[TypedPipe[String]]

    // if running from the assembly, need to explicitly tell it to use java classpath
    if (args.contains("--repl")) command.settings.usejavacp.value = true

    command.settings.classpath.append(System.getProperty("java.class.path"))

    // Force the repl to be synchronous, so all cmds are executed in the same thread
    command.settings.Yreplsync.value = true

    scaldingREPL = Some(new ScaldingILoop)
    ReplImplicits.mode = mode
    scaldingREPL.get.process(command.settings)
  }

  // This both updates the jobConf with hadoop arguments
  // and returns all the non-hadoop arguments. Should be called once if
  // you want to process hadoop arguments (like -libjars).
  protected def nonHadoopArgsFrom(args: Array[String]): Array[String] =
    (new GenericOptionsParser(conf, args)).getRemainingArgs

  /**
   * Sets the mode for this job, updates jobConf with hadoop arguments
   * and returns all the non-hadoop arguments.
   *
   * @param args from the command line.
   * @return a Mode for the job (e.g. local, hdfs), and the non-hadoop params
   */
  def parseModeArgs(args: Array[String]): (Mode, Array[String]) = {
    val a = nonHadoopArgsFrom(args)
    (Mode(Args(a), conf), a)
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
