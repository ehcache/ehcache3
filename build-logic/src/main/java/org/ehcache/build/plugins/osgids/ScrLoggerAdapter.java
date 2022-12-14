/**
 * Copyright (C) 2016 Elmar Schug <elmar.schug@jayware.org>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ehcache.build.plugins.osgids;

import org.apache.felix.scrplugin.Log;
import org.gradle.api.logging.Logger;


final class ScrLoggerAdapter implements Log
{
    private final Logger logger;

    ScrLoggerAdapter(Logger logger) {
        this.logger = logger;
    }

    @Override
    public boolean isDebugEnabled()
    {
        return logger.isDebugEnabled();
    }

    @Override
    public void debug(String content)
    {
        logger.debug(content);
    }

    @Override
    public void debug(String content, Throwable error)
    {
        logger.debug(content, error);
    }

    @Override
    public void debug(Throwable error)
    {
        logger.debug(error.toString());
    }

    @Override
    public boolean isInfoEnabled()
    {
        return logger.isInfoEnabled();
    }

    @Override
    public void info(String content)
    {
        logger.info(content);
    }

    @Override
    public void info(String content, Throwable error)
    {
        logger.info(content, error);
    }

    @Override
    public void info(Throwable error)
    {
        logger.info(error.toString());
    }

    @Override
    public boolean isWarnEnabled()
    {
        return logger.isWarnEnabled();
    }

    @Override
    public void warn(String content)
    {
        logger.warn(content);
    }

    @Override
    public void warn(String content, String location, int lineNumber)
    {
        logger.warn("{} [{},{}]", content, location, lineNumber);
    }

    @Override
    public void warn(String content, String location, int lineNumber, int columNumber)
    {
        logger.warn("{} [{},{}:{}]", content, location, lineNumber, columNumber);
    }

    @Override
    public void warn(String content, Throwable error)
    {
        logger.warn(content, error);
    }

    @Override
    public void warn(Throwable error)
    {
        logger.warn(error.toString());
    }

    @Override
    public boolean isErrorEnabled()
    {
        return logger.isErrorEnabled();
    }

    @Override
    public void error(String content)
    {
        logger.error(content);
    }

    @Override
    public void error(String content, String location, int lineNumber)
    {
        logger.error("{} [{},}{}]", content, location, lineNumber);
    }

    @Override
    public void error(String content, String location, int lineNumber, int columnNumber)
    {
        logger.error("{} [{},{}:{}]", content, location, lineNumber, columnNumber);
    }

    @Override
    public void error(String content, Throwable error)
    {
        logger.error(content, error);
    }

    @Override
    public void error(Throwable error)
    {
        logger.error(error.toString());
    }
}
