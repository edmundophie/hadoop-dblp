package com.edmundophie.dblp;

import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by edmundophie on 12/14/15.
 */
public class DBLPHandler extends DefaultHandler {
    private int articleCount;
    private int inproceedingsCount;
    private int phdThesisCount;
    private int masterThesisCount;
    private List<String> authorNames = new ArrayList();
    private boolean bAuthor = false;

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) {
        if(qName.equalsIgnoreCase("author")) {
            bAuthor = true;
        }
        else if(qName.equalsIgnoreCase("article")) {
            ++articleCount;
        }
        else if(qName.equalsIgnoreCase("inproceedings")) {
            ++inproceedingsCount;
        }
        else if(qName.equalsIgnoreCase("phdthesis")) {
            ++phdThesisCount;
        }
        else if(qName.equalsIgnoreCase("mastersthesis")) {
            ++masterThesisCount;
        }
    }

    @Override
    public void characters(char ch[], int start, int length) {
        if(bAuthor) {
            String authorName = new String(ch, start, length);
            authorNames.add(authorName);
        }
    }

    public int getArticleCount() {
        return articleCount;
    }

    public int getInproceedingsCount() {
        return inproceedingsCount;
    }

    public int getPhdThesisCount() {
        return phdThesisCount;
    }

    public int getMasterThesisCount() {
        return masterThesisCount;
    }

    public List<String> getAuthorNames() {
        return authorNames;
    }
}
