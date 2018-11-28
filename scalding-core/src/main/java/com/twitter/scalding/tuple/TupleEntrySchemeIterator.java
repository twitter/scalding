/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.scalding.tuple;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import cascading.flow.FlowProcess;
import cascading.scheme.ConcreteCall;
import cascading.scheme.Scheme;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleEntrySchemeIteratorProps;
import cascading.tuple.TupleException;
import cascading.tuple.Tuples;
import cascading.util.CloseableIterator;
import cascading.util.SingleCloseableInputIterator;
import cascading.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class TupleEntrySchemeIterator is a helper class for wrapping a {@link Scheme} instance, calling
 * {@link Scheme#source(cascading.flow.FlowProcess, cascading.scheme.SourceCall)} on every call to
 * {@link #next()}. The behavior can be controlled via properties defined in {@link TupleEntrySchemeIteratorProps}.
 * <p/>
 * Use this class inside a custom {@link cascading.tap.Tap} when overriding the
 * {@link cascading.tap.Tap#openForRead(cascading.flow.FlowProcess)} method.
 */
public class TupleEntrySchemeIterator<Config, Input> extends TupleEntryIterator
{
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( cascading.tuple.TupleEntrySchemeIterator.class );

  private final FlowProcess<Config> flowProcess;
  private final Scheme scheme;
  private final CloseableIterator<Input> inputIterator;
  private final Set<Class<? extends Exception>> permittedExceptions;
  private ConcreteCall sourceCall;

  private String identifier;
  private boolean isComplete = false;
  private boolean hasWaiting = false;
  private TupleException currentException;

  public TupleEntrySchemeIterator( FlowProcess<Config> flowProcess, Scheme scheme, Input input )
  {
    this( flowProcess, scheme, input, null );
  }

  public TupleEntrySchemeIterator( FlowProcess<Config> flowProcess, Scheme scheme, Input input, String identifier )
  {
    this( flowProcess, scheme, (CloseableIterator<Input>) new SingleCloseableInputIterator( (Closeable) input ), identifier );
  }

  public TupleEntrySchemeIterator( FlowProcess<Config> flowProcess, Scheme scheme, CloseableIterator<Input> inputIterator )
  {
    this( flowProcess, scheme, inputIterator, null );
  }

  public TupleEntrySchemeIterator( FlowProcess<Config> flowProcess, Scheme scheme, CloseableIterator<Input> inputIterator, String identifier )
  {
    super( scheme.getSourceFields() );
    this.flowProcess = flowProcess;
    this.scheme = scheme;
    this.inputIterator = inputIterator;
    this.identifier = identifier;

    Object permittedExceptions = flowProcess.getProperty( TupleEntrySchemeIteratorProps.PERMITTED_EXCEPTIONS );

    if( permittedExceptions != null )
      this.permittedExceptions = Util.asClasses( permittedExceptions.toString(), "unable to load permitted exception class" );
    else
      this.permittedExceptions = Collections.emptySet();

    if( this.identifier == null || this.identifier.isEmpty() )
      this.identifier = "'unknown'";

    if( !inputIterator.hasNext() )
    {
      isComplete = true;
      return;
    }

    sourceCall = new ConcreteCall();

    sourceCall.setIncomingEntry( getTupleEntry() );
    sourceCall.setInput( wrapInput( inputIterator.next() ) );

    try
    {
      this.scheme.sourcePrepare( flowProcess, sourceCall );
    }
    catch( IOException exception )
    {
      throw new TupleException( "unable to prepare source for input identifier: " + this.identifier, exception );
    }
  }

  protected FlowProcess<Config> getFlowProcess()
  {
    return flowProcess;
  }

  protected Input wrapInput( Input input )
  {
    return input;
  }

  @Override
  public boolean hasNext()
  {
    if( isComplete )
      return false;

    if( hasWaiting )
      return true;

    try
    {
      getNext();
    }
    catch( Exception exception )
    {
      if( identifier == null || identifier.isEmpty() )
        identifier = "'unknown'";

      if( permittedExceptions.contains( exception.getClass() ) )
      {
        LOG.warn( "Caught permitted exception while reading {}", identifier, exception );
        return false;
      }

      currentException = new TupleException( "unable to read from input identifier: " + identifier, exception );

      return true;
    }

    if( !hasWaiting )
      isComplete = true;

    return !isComplete;
  }

  private TupleEntry getNext() throws IOException
  {
    Tuples.asModifiable( sourceCall.getIncomingEntry().getTuple() );
    hasWaiting = scheme.source( flowProcess, sourceCall );

    if( !hasWaiting && inputIterator.hasNext() )
    {
      sourceCall.setInput( wrapInput( inputIterator.next() ) );
      scheme.sourcePrepare(flowProcess, sourceCall);

      return getNext();
    }

    return getTupleEntry();
  }

  @Override
  public TupleEntry next()
  {
    try
    {
      if( currentException != null )
        throw currentException;
    }
    finally
    {
      currentException = null; // data may be trapped
    }

    if( isComplete )
      throw new IllegalStateException( "no next element" );

    try
    {
      if( hasWaiting )
        return getTupleEntry();

      return getNext();
    }
    catch( Exception exception )
    {
      throw new TupleException( "unable to source from input identifier: " + identifier, exception );
    }
    finally
    {
      hasWaiting = false;
    }
  }

  @Override
  public void remove()
  {
    throw new UnsupportedOperationException( "may not remove elements from this iterator" );
  }

  @Override
  public void close() throws IOException
  {
    try
    {
      if( sourceCall != null )
        scheme.sourceCleanup( flowProcess, sourceCall );
    }
    finally
    {
      inputIterator.close();
    }
  }
}
