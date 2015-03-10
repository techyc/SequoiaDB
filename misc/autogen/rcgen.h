#ifndef RCGen_H
#define RCGen_H

#include <string>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>
#include <boost/foreach.hpp>

#define RCALIGN 32
#define RCXMLSRC "rclist.xml"
#define CPPPATH "../../SqlDB/engine/oss/ossErr.cpp"
#define CPATH "../../SqlDB/engine/include/ossErr.h"
#define CSPATH "../../driver/C#.Net/Driver/exception/Errors.cs"
#define JAVAPATH "../../driver/java/src/main/resources/errors.properties"
#define JSPATH "../../SqlDB/engine/spt/error.js"
#define WEBPATH "../../client/admin/admintpl/error_"
#define PYTHONPATH "../../driver/python/pysqldb/err.prop"
#define WEBPATHSUFFIX ".php"
#define DOCPATH "../../doc/references/exceptionmapping/topics/exceptionmapping_"
#define DOCPATHSUFFIX ".dita"
#define CONSLIST "rclist.conslist"
#define CODELIST "rclist.codelist"
#define NAME "name"
#define VALUE "value"
#define DESCRIPTION "description"


class RCGen
{
    const char* language;
    std::vector<std::pair<std::string, int> > conslist;
    std::vector<std::pair<std::string, std::string> > codelist;
    void loadFromXML ();
    void genC ();
    void genCPP ();
    void genCS ();
    void genJava ();
    void genJS () ;
    void genPython() ;
public:
    RCGen (const char* lang);
    void run ();
    void genDoc () ;
    void genWeb () ;
};
#endif
