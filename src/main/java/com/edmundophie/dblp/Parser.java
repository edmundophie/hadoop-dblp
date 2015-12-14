package com.edmundophie.dblp;

import org.apache.hadoop.io.IntWritable;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Created by edmundophie on 12/14/15.
 */
public class Parser {
    DBLPHandler dblpHandler;

    public Parser(String textToParse) {
        try {
            SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();
            SAXParser saxParser = saxParserFactory.newSAXParser();

            InputStream textToParseStream = new ByteArrayInputStream(textToParse.getBytes(StandardCharsets.UTF_8));
            dblpHandler = new DBLPHandler();
            saxParser.parse(textToParseStream, dblpHandler);
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public IntWritable getArticleCount() {
        return new IntWritable(dblpHandler.getArticleCount());
    }

    public IntWritable getInproceedingsCount() {
        return new IntWritable(dblpHandler.getInproceedingsCount());
    }

    public IntWritable getPhdThesisCount() {
        return new IntWritable(dblpHandler.getPhdThesisCount());
    }

    public IntWritable getMasterThesisCount() {
        return new IntWritable(dblpHandler.getMasterThesisCount());
    }

    public List<String> getAuthorNames() {
        return dblpHandler.getAuthorNames();
    }
}
