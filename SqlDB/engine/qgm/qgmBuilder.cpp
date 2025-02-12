/*******************************************************************************


   Copyright (C) 2011-2014 SqlDB Ltd.

   This program is free software: you can redistribute it and/or modify
   it under the term of the GNU Affero General Public License, version 3,
   as published by the Free Software Foundation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warrenty of
   MARCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program. If not, see <http://www.gnu.org/license/>.

   Source File Name = qgmBuilder.cpp

   Descriptive Name =

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains functions for agent processing.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  YW  Initial Draft

   Last Changed =

******************************************************************************/

#include "qgmBuilder.hpp"
#include "pd.hpp"
#include "qgmOptiSelect.hpp"
#include "qgmOptiNLJoin.hpp"
#include "qgmConditionNode.hpp"
#include "qgmOptiInsert.hpp"
#include "qgmPtrTable.hpp"
#include "qgmParamTable.hpp"
#include "qgmPlan.hpp"
#include "qgmPlReturn.hpp"
#include "qgmPlScan.hpp"
#include "qgmPlFilter.hpp"
#include "qgmPlInsert.hpp"
#include "qgmPlNLJoin.hpp"
#include "qgmOptiSort.hpp"
#include "qgmPlSort.hpp"
#include "qgmOptiCommand.hpp"
#include "qgmPlCommand.hpp"
#include "qgmOptiDelete.hpp"
#include "qgmPlDelete.hpp"
#include "qgmOptiUpdate.hpp"
#include "qgmPlUpdate.hpp"
#include "qgmPlAggregation.hpp"
#include "qgmOptiAggregation.hpp"
#include "qgmPlMthMatcherScan.hpp"
#include "qgmPlMthMatcherFilter.hpp"
#include "qgmOptiMthMatchSelect.hpp"
#include "ossMem.hpp"
#include "qgmPlSplitBy.hpp"
#include "qgmOptiSplit.hpp"
#include "qgmPlHashJoin.hpp"
#include "pdTrace.hpp"
#include "qgmTrace.hpp"

#define QGM_ALIAS_ASSERT( alias, len ) \
        SDB_ASSERT( ((NULL != alias) && (0 != len))\
                     | ((NULL == alias) && (0 == len)),\
                     "impossible" ) ;

#define QGM_VALUEPTR( itr )\
        (&(*((itr)->value.begin())))
#define QGM_VALUESIZE(itr)\
        ( (itr)->value.end() - (itr)->value.begin() )

namespace engine
{
   BOOLEAN isJoin( INT32 type )
   {
      return ( SQL_GRAMMAR::INNERJOIN == type
                || SQL_GRAMMAR::L_OUTERJOIN == type
                || SQL_GRAMMAR::R_OUTERJOIN == type
                || SQL_GRAMMAR::F_OUTERJOIN == type ) ;
   }

   _qgmBuilder::_qgmBuilder( _qgmPtrTable *ptrT, _qgmParamTable *paramT )
   :_table( ptrT ),
    _param( paramT )
   {
      SDB_ASSERT( NULL != _table && NULL != _param,
                  "impossible" ) ;
   }

   _qgmBuilder::~_qgmBuilder()
   {

   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMBUILDER_BUILDORDERBY, "_qgmBuilder::buildOrderby" )
   BSONObj _qgmBuilder::buildOrderby( const qgmOPFieldVec &orderby )
   {
      PD_TRACE_ENTRY( SDB__QGMBUILDER_BUILDORDERBY ) ;
      BSONObjBuilder builder ;
      qgmOPFieldVec::const_iterator itr = orderby.begin() ;
      INT32 asc = 1 ;
      UINT32 desc = -1 ;
      string str ;
      for ( ; itr != orderby.end(); itr++ )
      {
         if ( SQL_GRAMMAR::ASC == itr->type )
         {
            builder.append( itr->value.attr().toString(), asc ) ;
         }
         else
         {
            builder.append( itr->value.attr().toString(), desc ) ;
         }
      }
      PD_TRACE_EXIT( SDB__QGMBUILDER_BUILDORDERBY ) ;
      return builder.obj() ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMBUILDER_BUILD1, "_qgmBuilder::build" )
   INT32 _qgmBuilder::build( const SQL_CONTAINER &tree,
                             _qgmOptiTreeNode *&node )
   {
      PD_TRACE_ENTRY( SDB__QGMBUILDER_BUILD1 ) ;
      INT32 rc = SDB_OK ;
      if ( 1 != tree.size() )
      {
         PD_LOG( PDDEBUG, "invalid size of tree:%d", tree.size() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      rc = _build( tree.begin(), node ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

   done:
      PD_TRACE_EXITRC( SDB__QGMBUILDER_BUILD1, rc ) ;
      return rc ;
   error:
      if ( NULL != node )
      {
         delete node ;
         node = NULL ;
      }
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMBUILDER_BUILD2, "_qgmBuilder::build2" )
   INT32 _qgmBuilder::build( _qgmOptiTreeNode *logicalTree,
                             _qgmPlan *&physicalTree )
   {
      PD_TRACE_ENTRY( SDB__QGMBUILDER_BUILD2 ) ;
      INT32 rc = SDB_OK ;
      SDB_ASSERT( NULL != logicalTree && NULL == physicalTree,
                  "impossible" ) ;

      rc = _buildPhysicalNode( logicalTree,
                               physicalTree ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB__QGMBUILDER_BUILD2, rc ) ;
      return rc ;
   error:
      SAFE_OSS_DELETE( physicalTree ) ;
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMBUILDER__BUILDPHYNODE, "_qgmBuilder::_buildPhysicalNode" )
   INT32 _qgmBuilder::_buildPhysicalNode( _qgmOptiTreeNode *logicalTree,
                                          _qgmPlan *&physicalTree )
   {
      PD_TRACE_ENTRY( SDB__QGMBUILDER__BUILDPHYNODE ) ;
      INT32 rc = SDB_OK ;
      SDB_ASSERT( NULL != logicalTree, "impossible" ) ;

      switch ( logicalTree->getType() )
      {
      case QGM_OPTI_TYPE_SORT:
      {
         if ( NULL == physicalTree )
         {
            physicalTree = SDB_OSS_NEW _qgmPlReturn() ;
            if ( NULL == physicalTree )
            {
               PD_LOG( PDERROR, "failed to allocate mem." ) ;
               rc = SDB_OOM ;
               goto error ;
            }

            rc = _buildPhysicalNode( logicalTree, physicalTree ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
         }
         else
         {
           _qgmPlan *phy = NULL ;
            rc  = _crtPhySort( (_qgmOptiSort *)logicalTree,
                                physicalTree,
                                phy ) ;
            if ( SDB_OK != rc )
            {
                goto error ;
            }

            SDB_ASSERT( NULL != phy, "impossible" ) ;

            {
            _qgmOptiTreeNode *logic = logicalTree->getSubNode( 0 ) ;
            rc = _buildPhysicalNode( logic, phy ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
            }
         }
         break ;
      }
      case QGM_OPTI_TYPE_FILTER:
      {
         if ( NULL == physicalTree )
         {
            physicalTree = SDB_OSS_NEW _qgmPlReturn() ;
            if ( NULL == physicalTree )
            {
               PD_LOG( PDERROR, "failed to allocate mem." ) ;
               rc = SDB_OOM ;
               goto error ;
            }

            rc = _buildPhysicalNode( logicalTree, physicalTree ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
         }
         else
         {
            _qgmPlan *phy = NULL ;
            rc = _crtPhyFilter( ( _qgmOptiSelect *)logicalTree,
                                  physicalTree,
                                  phy ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }

            SDB_ASSERT( NULL != phy, "impossible" ) ;
            {
            _qgmOptiTreeNode *logic = logicalTree->getSubNode( 0 ) ;
            rc = _buildPhysicalNode( logic, phy ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
            }
         }

         break ;
      }
      case QGM_OPTI_TYPE_AGGR:
      {
         if ( NULL == physicalTree )
         {
            physicalTree = SDB_OSS_NEW _qgmPlReturn() ;
            if ( NULL == physicalTree )
            {
               PD_LOG( PDERROR, "failed to allocate mem." ) ;
               rc = SDB_OOM ;
               goto error ;
            }

            rc = _buildPhysicalNode( logicalTree, physicalTree ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
         }
         else
         {
            _qgmPlan *phy = NULL ;
            rc = _addPhyAggr( (_qgmOptiAggregation *)logicalTree,
                              physicalTree, phy ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }

            {
            rc = _buildPhysicalNode( logicalTree->getSubNode(0),
                                     phy ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
            }
         }
         break ;
      }
      case QGM_OPTI_TYPE_SCAN :
      {
         if ( NULL == physicalTree )
         {
            physicalTree = SDB_OSS_NEW _qgmPlReturn() ;
            if ( NULL == physicalTree )
            {
               PD_LOG( PDERROR, "failed to allocate mem." ) ;
               rc = SDB_OOM ;
               goto error ;
            }

            rc = _buildPhysicalNode( logicalTree, physicalTree ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
         }
         else
         {
            rc = _addPhyScan( ( _qgmOptiSelect *)logicalTree,
                                physicalTree ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
         }

         break ;
      }
      case QGM_OPTI_TYPE_JOIN :
      {
         SDB_ASSERT( NULL != physicalTree, "impossible" ) ;
         SDB_ASSERT( 2 == logicalTree->getSubNodeCount(),
                     "impossible" ) ;

         _qgmPlan *phy = NULL ;
         _qgmOptiNLJoin *join = (_qgmOptiNLJoin *)logicalTree ;
         rc = _crtPhyJoin( join,
                           physicalTree,
                           phy ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         else
         {
            rc = _buildPhysicalNode( join->outer(), phy ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }

            rc = _buildPhysicalNode( join->inner(), phy ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
         }

         if (!join->_hints.empty() )
         {
            rc = (( _qgmPlHashJoin *)phy)->init( join ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
         }
         break ;
      }
      case QGM_OPTI_TYPE_INSERT :
      {
         _qgmOptiInsert *insert = ( _qgmOptiInsert *)logicalTree ;
         _qgmPlInsert *plInsert = SDB_OSS_NEW
                                  _qgmPlInsert( insert->_collection.value ) ;
         if ( NULL == plInsert )
         {
            PD_LOG( PDERROR, "failed to allocate mem." ) ;
            rc = SDB_OOM ;
            goto error ;
         }

         physicalTree = plInsert ;

         if ( !insert->_columns.empty() )
         {
            plInsert->addCV( insert->_columns, insert->_values ) ;
         }
         else
         {
            rc = _buildPhysicalNode( logicalTree->getSubNode( 0 ),
                                     physicalTree ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
         }
         break ;
      }
      case QGM_OPTI_TYPE_UPDATE:
      {
         _qgmOptiUpdate *up = ( _qgmOptiUpdate* )logicalTree ;
         _qgmPlUpdate *update = SDB_OSS_NEW
                                _qgmPlUpdate( up->_collection,
                                              up->_columns,
                                              up->_values,
                                              up->_condition ) ;
         if ( NULL == update )
         {
            PD_LOG( PDERROR, "failed to allocate mem." ) ;
            rc = SDB_OOM ;
            goto error ;
         }

         physicalTree = update ;
         break ;
      }
      case QGM_OPTI_TYPE_DELETE :
      {
         _qgmOptiDelete *del = ( _qgmOptiDelete * )logicalTree ;
         _qgmPlDelete *phyd = SDB_OSS_NEW
                             _qgmPlDelete( del->_collection,
                                           del->_condition ) ;
         if ( NULL == phyd )
         {
            PD_LOG( PDERROR, "failed to allocate mem." ) ;
            rc = SDB_OOM ;
            goto error ;
         }
         physicalTree = phyd ;
         break ;
      }
      case QGM_OPTI_TYPE_COMMAND :
      {
         rc = _addPhyCommand( (_qgmOptiCommand *)logicalTree,
                               physicalTree ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         break ;
      }
      case QGM_OPTI_TYPE_MTHMCHSCAN :
      {
         if ( NULL == physicalTree )
         {
            physicalTree = SDB_OSS_NEW _qgmPlReturn() ;
            if ( NULL == physicalTree )
            {
               PD_LOG( PDERROR, "failed to allocate mem." ) ;
               rc = SDB_OOM ;
               goto error ;
            }

            rc = _buildPhysicalNode( logicalTree, physicalTree ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
         }
         else
         {
            rc = _addMthMatcherScan( ( qgmOptiMthMatchSelect *)logicalTree,
                                       physicalTree ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
         }

         break ;
      }
      case QGM_OPTI_TYPE_MTHMCHFILTER:
      {
         if ( NULL == physicalTree )
         {
            physicalTree = SDB_OSS_NEW _qgmPlReturn() ;
            if ( NULL == physicalTree )
            {
               PD_LOG( PDERROR, "failed to allocate mem." ) ;
               rc = SDB_OOM ;
               goto error ;
            }

            rc = _buildPhysicalNode( logicalTree, physicalTree ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
         }
         else
         {
            _qgmPlan *phy = NULL ;
            rc = _crtMthMatcherFilter( ( qgmOptiMthMatchSelect *)logicalTree,
                                        physicalTree,
                                        phy ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }

            SDB_ASSERT( NULL != phy, "impossible" ) ;
            {
            _qgmOptiTreeNode *logic = logicalTree->getSubNode( 0 ) ;
            rc = _buildPhysicalNode( logic, phy ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
            }
         }

         break ;
      }
      case QGM_OPTI_TYPE_SPLIT:
      {
         if ( NULL == physicalTree )
         {
            physicalTree = SDB_OSS_NEW _qgmPlReturn() ;
            if ( NULL == physicalTree )
            {
               PD_LOG( PDERROR, "failed to allocate mem." ) ;
               rc = SDB_OOM ;
               goto error ;
            }

            rc = _buildPhysicalNode( logicalTree, physicalTree ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
         }
         else
         {
            SDB_ASSERT( 1 == logicalTree->getSubNodeCount(), "impossible" ) ;
            _qgmPlan *split = SDB_OSS_NEW
                         _qgmPlSplitBy( ((_qgmOptiSplit *)logicalTree)->_splitby,
                                        logicalTree->getSubNode(0)->getAlias()  ) ;
            if ( NULL == split )
            {
               PD_LOG( PDERROR, "failed to allocate mem." ) ;
               rc = SDB_OOM ;
               goto error ;
            }

            rc = physicalTree->addChild( split ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }

            rc = _buildPhysicalNode( logicalTree->getSubNode(0), split ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
         }

         break ;
      }
      default:
      {
         PD_LOG( PDERROR, "unknown opti type:%d",
                 logicalTree->getType() ) ;
         rc = SDB_SYS ;
         goto error ;
      }
      }

   done:
      PD_TRACE_EXITRC( SDB__QGMBUILDER__BUILDPHYNODE, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMBUILDER__ADDPHYCOMMAND, "_qgmBuilder::_addPhyCommand" )
   INT32 _qgmBuilder::_addPhyCommand( _qgmOptiCommand *command,
                                      _qgmPlan *&father )
   {
      PD_TRACE_ENTRY( SDB__QGMBUILDER__ADDPHYCOMMAND ) ;
      SDB_ASSERT( NULL == father, "impossible" ) ;
      INT32 rc = SDB_OK ;
      BSONObj partition ;
      if ( !command->_partition.empty() )
      {
         partition = buildOrderby( command->_partition ) ;
      }
      _qgmPlan *phyc = SDB_OSS_NEW
                       _qgmPlCommand( command->_commandType,
                                      command->_fullName,
                                      command->_indexName,
                                      command->_indexColumns,
                                      partition,
                                      command->_uniqIndex ) ;
      if ( NULL == phyc )
      {
         PD_LOG( PDERROR, "failed to allocate mem." ) ;
         rc = SDB_OOM ;
         goto error ;
      }

      if ( SQL_GRAMMAR::LISTCS == command->_commandType
           || SQL_GRAMMAR::LISTCL == command->_commandType )
      {
         father = SDB_OSS_NEW _qgmPlReturn() ;
         if ( NULL == father )
         {
            PD_LOG( PDERROR, "failed to allocate mem." ) ;
            rc = SDB_OOM ;
            goto error ;
         }

         rc = father->addChild( phyc ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
      else
      {
         father = phyc ;
      }

   done:
      PD_TRACE_EXITRC( SDB__QGMBUILDER__ADDPHYCOMMAND, rc ) ;
      return rc ;
   error:
      SAFE_OSS_DELETE( phyc ) ;
      SAFE_OSS_DELETE( father ) ;
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMBUILDER__ADDPHYAGGR, "_qgmBuilder::_addPhyAggr" )
   INT32 _qgmBuilder::_addPhyAggr( _qgmOptiAggregation *aggr,
                                   _qgmPlan *father,
                                   _qgmPlan *&pyh )
   {
      PD_TRACE_ENTRY( SDB__QGMBUILDER__ADDPHYAGGR ) ;
      INT32 rc = SDB_OK ;
      pyh = SDB_OSS_NEW _qgmPlAggregation( aggr->_selector,
                                           aggr->_groupby,
                                           aggr->_alias,
                                           _table ) ;
      if ( NULL == pyh )
      {
         PD_LOG( PDERROR, "failed to allocate mem." ) ;
         rc = SDB_OOM ;
         goto error ;
      }

      if ( !pyh->ready() )
      {
         PD_LOG( PDDEBUG, "failed to initialize aggregation" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      rc = father->addChild( pyh ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB__QGMBUILDER__ADDPHYAGGR, rc ) ;
      return rc ;
   error:
      SAFE_OSS_DELETE( pyh ) ;
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMBUILDER__CRTPHYSORT, "_qgmBuilder::_crtPhySort" )
   INT32 _qgmBuilder::_crtPhySort( _qgmOptiSort *sort,
                                   _qgmPlan *father,
                                   _qgmPlan *&phy )
   {
      PD_TRACE_ENTRY( SDB__QGMBUILDER__CRTPHYSORT ) ;
      INT32 rc = SDB_OK ;
      phy = SDB_OSS_NEW _qgmPlSort( sort->_orderby ) ;
      if ( NULL == phy )
      {
         PD_LOG( PDERROR, "failed to allocate mem." ) ;
         rc = SDB_OOM ;
         goto error ;
      }

      rc = father->addChild( phy ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

   done:
      PD_TRACE_EXITRC( SDB__QGMBUILDER__CRTPHYSORT, rc ) ;
      return rc ;
   error:
      SAFE_OSS_DELETE( phy ) ;
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMBUILDER__ADDPHYSCAN, "_qgmBuilder::_addPhyScan" )
   INT32 _qgmBuilder::_addPhyScan( _qgmOptiSelect *s,
                                   _qgmPlan *father )
   {
      PD_TRACE_ENTRY( SDB__QGMBUILDER__ADDPHYSCAN ) ;
      INT32 rc = SDB_OK ;

      BSONObj orderby = buildOrderby( s->_orderby ) ;
      _qgmPlan * pyh = SDB_OSS_NEW
                        _qgmPlScan( s->_collection.value,
                                    s->_selector,
                                    orderby,
                                    BSONObj(),
                                    s->_skip,
                                    s->_limit,
                                    s->_alias,
                                    s->_condition ) ;
      if ( NULL == pyh )
      {
         PD_LOG( PDERROR, "failed to allocate mem." ) ;
         rc = SDB_OOM ;
         goto error ;
      }

      rc = father->addChild( pyh ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      s->_condition = NULL ;
   done:
      PD_TRACE_EXITRC( SDB__QGMBUILDER__ADDPHYSCAN, rc ) ;
      return rc ;
   error:
      SAFE_OSS_DELETE( pyh ) ;
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMBUILDER__ADDMTHMATHERSCAN, "_qgmBuilder::_addMthMatcherScan" )
   INT32 _qgmBuilder::_addMthMatcherScan( qgmOptiMthMatchSelect *s,
                                          _qgmPlan *father )
   {
      PD_TRACE_ENTRY( SDB__QGMBUILDER__ADDMTHMATHERSCAN ) ;
      INT32 rc = SDB_OK ;
      qgmPlMthMatcherScan *pScan = NULL;
      BSONObj orderby = buildOrderby( s->_orderby ) ;
      pScan = SDB_OSS_NEW qgmPlMthMatcherScan( s->_collection.value,
                                             s->_selector,
                                             orderby,
                                             BSONObj(),
                                             s->_skip,
                                             s->_limit,
                                             s->_alias,
                                             s->_matcher ) ;
      if ( NULL == pScan )
      {
         PD_LOG( PDERROR, "failed to allocate mem." ) ;
         rc = SDB_OOM ;
         goto error ;
      }

      rc = father->addChild( pScan ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      s->_condition = NULL ;
   done:
      PD_TRACE_EXITRC( SDB__QGMBUILDER__ADDMTHMATHERSCAN, rc ) ;
      return rc ;
   error:
      SAFE_OSS_DELETE( pScan ) ;
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMBUILDER__CRTPHYJOIN, "_qgmBuilder::_crtPhyJoin" )
   INT32 _qgmBuilder::_crtPhyJoin( _qgmOptiNLJoin *join,
                                   _qgmPlan *father,
                                   _qgmPlan *&phy )
   {
      PD_TRACE_ENTRY( SDB__QGMBUILDER__CRTPHYJOIN ) ;
      INT32 rc = SDB_OK ;

      if ( join->_hints.empty() )
      {
         phy = SDB_OSS_NEW _qgmPlNLJoin( join->joinType() ) ;
         if ( NULL == phy )
         {
            PD_LOG( PDERROR, "failed to allocate mem." ) ;
            rc = SDB_OOM ;
            goto error ;
         }
      }
      else
      {
         phy = SDB_OSS_NEW _qgmPlHashJoin( join->joinType() ) ;
         if ( NULL == phy )
         {
            PD_LOG( PDERROR, "failed to allocate mem." ) ;
            rc = SDB_OOM ;
            goto error ;
         }
      }

      rc = father->addChild( phy ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      phy->setParamTable( _param ) ;
      phy->setVar( join->_varList ) ;
   done:
      PD_TRACE_EXITRC( SDB__QGMBUILDER__CRTPHYJOIN, rc ) ;
      return rc ;
   error:
      SAFE_OSS_DELETE( phy ) ;
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMBUILDER__CRTPHYFILTER, "_qgmBuilder::_crtPhyFilter" ) ;
   INT32 _qgmBuilder::_crtPhyFilter( _qgmOptiSelect *s,
                                     _qgmPlan *father,
                                     _qgmPlan *&phy )
   {
      PD_TRACE_ENTRY( SDB__QGMBUILDER__CRTPHYFILTER ) ;
      INT32 rc = SDB_OK ;

      phy = SDB_OSS_NEW
            _qgmPlFilter( s->_selector,
                          s->_condition,
                          s->_skip,
                          s->_limit,
                          s->_alias ) ;
      if ( NULL == phy )
      {
         PD_LOG( PDERROR, "failed to allocate mem." ) ;
         rc = SDB_OOM ;
         goto error ;
      }

      rc = father->addChild( phy ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      s->_condition = NULL ;
   done:
      PD_TRACE_EXITRC( SDB__QGMBUILDER__CRTPHYFILTER, rc ) ;
      return rc ;
   error:
      SAFE_OSS_DELETE( phy ) ;
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMBUILDER__CRTMTHMATHERFILTER, "_qgmBuilder::_crtMthMatcherFilter" )
   INT32 _qgmBuilder::_crtMthMatcherFilter( qgmOptiMthMatchSelect *s,
                                             _qgmPlan *father,
                                             _qgmPlan *&phy )
   {
      PD_TRACE_ENTRY( SDB__QGMBUILDER__CRTMTHMATHERFILTER ) ;
      INT32 rc = SDB_OK ;
      qgmPlMthMatcherFilter *pFilter = NULL;

      pFilter = SDB_OSS_NEW
            qgmPlMthMatcherFilter( s->_selector,
                          s->_skip,
                          s->_limit,
                          s->_alias ) ;
      if ( NULL == pFilter )
      {
         PD_LOG( PDERROR, "failed to allocate mem." ) ;
         rc = SDB_OOM ;
         goto error ;
      }
      rc = pFilter->loadPattern( s->_matcher );
      PD_RC_CHECK( rc, PDERROR,
                  "failed to load pattern(rc=%d)", rc );

      rc = father->addChild( pFilter ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      s->_condition = NULL ;
      phy = pFilter;
   done:
      PD_TRACE_EXITRC( SDB__QGMBUILDER__CRTMTHMATHERFILTER, rc ) ;
      return rc ;
   error:
      SAFE_OSS_DELETE( pFilter ) ;
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMBUILDER__BUILD1, "_qgmBuilder::_build" )
   INT32 _qgmBuilder::_build( const SQL_CON_ITR &root,
                              _qgmOptiTreeNode *&node )
   {
      PD_TRACE_ENTRY( SDB__QGMBUILDER__BUILD1 ) ;
      INT32 rc = SDB_OK ;
      INT32 type = (INT32)(root->value.id().to_long()) ;

      switch ( type )
      {
      case SQL_GRAMMAR::SQL :
      {
          break ;
      }
      case SQL_GRAMMAR::SELECT :
      {
         _qgmOptiSelect *select = SDB_OSS_NEW
                                  _qgmOptiSelect( _table, _param ) ;
         if ( NULL == select )
         {
            PD_LOG( PDERROR, "failed to allocate mem" ) ;
            rc = SDB_OOM ;
            goto error ;
         }

         node = select ;
         rc = _buildSelect( root, select ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }

         break ;
      }
      case SQL_GRAMMAR::INSERT :
      {
         _qgmOptiInsert *insert = SDB_OSS_NEW
                                  _qgmOptiInsert( _table, _param ) ;
         if ( NULL == insert )
         {
            PD_LOG( PDERROR, "failed to allocate mem" ) ;
            rc = SDB_OOM ;
            goto error ;
         }

         node = insert ;
         rc = _buildInsert( root, insert ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }

         break ;
      }
      case SQL_GRAMMAR::UPDATE :
      {
         _qgmOptiUpdate *update = SDB_OSS_NEW
                                  _qgmOptiUpdate( _table, _param ) ;
         if ( NULL == update )
         {
            PD_LOG( PDERROR, "failed to allocate mem" ) ;
            rc = SDB_OOM ;
            goto error ;
         }

         node = update ;
         rc = _buildUpdate( root, update ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         break ;
      }
      case SQL_GRAMMAR::DELETE_ :
      {
         _qgmOptiDelete *del = SDB_OSS_NEW
                               _qgmOptiDelete( _table, _param ) ;
         if ( NULL == del )
         {
            PD_LOG( PDERROR, "failed to allocate mem" ) ;
            rc = SDB_OOM ;
            goto error ;
         }

         node = del ;
         rc = _buildDelete( root, del ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         break ;
      }
      case SQL_GRAMMAR::CRTCS :
      {
         _qgmOptiCommand *command = SDB_OSS_NEW
                                    _qgmOptiCommand( _table, _param ) ;
         if ( NULL == command )
         {
            PD_LOG( PDERROR, "failed to allocate mem" ) ;
            rc = SDB_OOM ;
            goto error ;
         }

         node = command ;
         rc = _buildCrtCS( root, command ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         break ;
      }
      case SQL_GRAMMAR::DROPCS :
      {
         _qgmOptiCommand *command = SDB_OSS_NEW
                                    _qgmOptiCommand( _table, _param ) ;
         if ( NULL == command )
         {
            PD_LOG( PDERROR, "failed to allocate mem" ) ;
            rc = SDB_OOM ;
            goto error ;
         }

         node = command ;
         rc = _buildDropCS( root, command ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         break ;
      }
      case SQL_GRAMMAR::CRTCL :
      {
         _qgmOptiCommand *command = SDB_OSS_NEW
                                    _qgmOptiCommand( _table, _param ) ;
         if ( NULL == command )
         {
            PD_LOG( PDERROR, "failed to allocate mem" ) ;
            rc = SDB_OOM ;
            goto error ;
         }

         node = command ;
         rc = _buildCrtCL( root, command ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         break ;
      }
      case SQL_GRAMMAR::DROPCL :
      {
         _qgmOptiCommand *command = SDB_OSS_NEW
                                    _qgmOptiCommand( _table, _param ) ;
         if ( NULL == command )
         {
            PD_LOG( PDERROR, "failed to allocate mem" ) ;
            rc = SDB_OOM ;
            goto error ;
         }

         node = command ;
         rc = _buildDropCL( root, command ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         break ;
      }
      case SQL_GRAMMAR::CRTINDEX :
      {
         _qgmOptiCommand *command = SDB_OSS_NEW
                                    _qgmOptiCommand( _table, _param ) ;
         if ( NULL == command )
         {
            PD_LOG( PDERROR, "failed to allocate mem" ) ;
            rc = SDB_OOM ;
            goto error ;
         }

         node = command ;
         rc = _buildCrtIndex( root, command ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         break ;
      }
      case SQL_GRAMMAR::DROPINDEX :
      {
         _qgmOptiCommand *command = SDB_OSS_NEW
                                    _qgmOptiCommand( _table, _param ) ;
         if ( NULL == command )
         {
            PD_LOG( PDERROR, "failed to allocate mem" ) ;
            rc = SDB_OOM ;
            goto error ;
         }

         node = command ;
         rc = _buildDropIndex( root, command ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         break ;
      }
      case SQL_GRAMMAR::LISTCS :
      {
         _qgmOptiCommand *command = SDB_OSS_NEW
                                    _qgmOptiCommand( _table, _param ) ;
         if ( NULL == command )
         {
            PD_LOG( PDERROR, "failed to allocate mem" ) ;
            rc = SDB_OOM ;
            goto error ;
         }

         node = command ;
         rc = _buildListCS( root, command ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         break ;
      }
      case SQL_GRAMMAR::LISTCL :
      {
          _qgmOptiCommand *command = SDB_OSS_NEW
                                    _qgmOptiCommand( _table, _param ) ;
         if ( NULL == command )
         {
            PD_LOG( PDERROR, "failed to allocate mem" ) ;
            rc = SDB_OOM ;
            goto error ;
         }

         node = command ;
         rc = _buildListCL( root, command ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         break ;
      }
      case SQL_GRAMMAR::BEGINTRAN :
      {
         _qgmOptiCommand *command = SDB_OSS_NEW
                                    _qgmOptiCommand( _table, _param ) ;
         if ( NULL == command )
         {
            PD_LOG( PDERROR, "failed to allocate mem" ) ;
            rc = SDB_OOM ;
            goto error ;
         }

         node = command ;
         command->_commandType = SQL_GRAMMAR::BEGINTRAN ;
         break ;
      }
      case SQL_GRAMMAR::ROLLBACK :
      {
         _qgmOptiCommand *command = SDB_OSS_NEW
                                    _qgmOptiCommand( _table, _param ) ;
         if ( NULL == command )
         {
            PD_LOG( PDERROR, "failed to allocate mem" ) ;
            rc = SDB_OOM ;
            goto error ;
         }

         node = command ;
         command->_commandType = SQL_GRAMMAR::ROLLBACK ;
         break ;
      }
      case SQL_GRAMMAR::COMMIT :
      {
         _qgmOptiCommand *command = SDB_OSS_NEW
                                    _qgmOptiCommand( _table, _param ) ;
         if ( NULL == command )
         {
            PD_LOG( PDERROR, "failed to allocate mem" ) ;
            rc = SDB_OOM ;
            goto error ;
         }

         node = command ;
         command->_commandType = SQL_GRAMMAR::COMMIT ;
         break ;
      }
      default :
      {
         PD_LOG( PDERROR, "unknown type:%d", type ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      }

   done:
      PD_TRACE_EXITRC( SDB__QGMBUILDER__BUILD1, rc ) ;
      return rc ;
   error:
      SAFE_OSS_DELETE(node) ;
      goto done ;
   }

   INT32 _qgmBuilder::_buildListCL( const SQL_CON_ITR &root,
                                    _qgmOptiCommand *node )
   {
      INT32 rc = SDB_OK ;
      node->_commandType = SQL_GRAMMAR::LISTCL ;
      return rc ;
   }

   INT32 _qgmBuilder::_buildListCS(  const SQL_CON_ITR &root,
                                    _qgmOptiCommand *node )
   {
      INT32 rc = SDB_OK ;
      node->_commandType = SQL_GRAMMAR::LISTCS ;
      return rc ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMBUILDER__BUILDUPDATE, "_qgmBuilder::_buildUpdate" )
   INT32 _qgmBuilder::_buildUpdate( const SQL_CON_ITR &root,
                                    _qgmOptiUpdate *update )
   {
      PD_TRACE_ENTRY( SDB__QGMBUILDER__BUILDUPDATE ) ;
      INT32 rc = SDB_OK ;
      SDB_ASSERT( 2 == root->children.size()
                  || 3 == root->children.size(), "impossible" ) ;

      SQL_CON_ITR itr = root->children.begin() ;
      rc = _table->getAttr( itr, update->_collection ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      rc = _addSet( ++itr, update ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      if ( root->children.end() != ++itr )
      {
         rc = _buildCondition( itr, update->_condition ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
   done:
      PD_TRACE_EXITRC( SDB__QGMBUILDER__BUILDUPDATE, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMBUILDER__ADDSET, "_qgmBuilder::_addSet" )
   INT32 _qgmBuilder::_addSet( const SQL_CON_ITR &root,
                                _qgmOptiUpdate *update )
   {
      PD_TRACE_ENTRY( SDB__QGMBUILDER__ADDSET ) ;
      INT32 rc = SDB_OK ;
      INT32 type = (INT32)(root->value.id().to_long()) ;
      if ( SQL_GRAMMAR::COMMA == type )
      {
         SDB_ASSERT( 2 == root->children.size(), "impossible" ) ;
         rc = _addSet( root->children.begin(), update ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }

         rc = _addSet( root->children.begin() + 1,
                       update ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
      else if ( SQL_GRAMMAR::EG == type )
      {
         SDB_ASSERT( 2 == root->children.size(), "impossible" ) ;
         _qgmDbAttr attr ;
         _qgmOpField f ;
         SQL_CON_ITR itr = root->children.begin() ;
         INT32 type = (INT32)((itr+1)->value.id().to_long()) ;
         rc = _table->getAttr( itr, attr ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }

         if ( SQL_GRAMMAR::DATE == type )
         {
            SDB_ASSERT( 1 == (itr+1)->children.size(), "impossible" ) ;
            rc = _table->getField( (itr+1)->children.begin(),
                                    f.value.attr()) ;
         }
         else
         {
            rc = _table->getField( itr + 1,
                                   f.value.attr() ) ;
         }

         if ( SDB_OK != rc )
         {
            goto error ;
         }

         f.type = type ;
         update->_columns.push_back( attr ) ;
         update->_values.push_back( f ) ;
      }
      else
      {
         PD_LOG( PDERROR, "invalid type:%d", type ) ;
         SDB_ASSERT( FALSE, "impossible" ) ;
         rc = SDB_SYS ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB__QGMBUILDER__ADDSET, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMBUILDER__BUILDDELETE, "_qgmBuilder::_buildDelete" )
   INT32 _qgmBuilder::_buildDelete( const SQL_CON_ITR &root,
                                    _qgmOptiDelete *node )
   {
      PD_TRACE_ENTRY( SDB__QGMBUILDER__BUILDDELETE ) ;
      INT32 rc = SDB_OK ;
      if ( root->children.empty() )
      {
         rc = _table->getAttr( root, node->_collection ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
      else if ( 2 == root->children.size() )
      {
         SQL_CON_ITR itr = root->children.begin() ;
         rc = _table->getAttr( itr, node->_collection ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         rc = _buildCondition( ++itr, node->_condition ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
      else
      {
         PD_LOG( PDERROR, "invalid children size:%d",
                 root->children.size() ) ;
         SDB_ASSERT( FALSE, "impossible" ) ;
         rc = SDB_SYS ;
         goto error ;
      }

   done:
      PD_TRACE_EXITRC( SDB__QGMBUILDER__BUILDDELETE, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMBUILDER__BUILDDROPCL, "_qgmBuilder::_buildDropCL" )
   INT32 _qgmBuilder::_buildDropCL( const SQL_CON_ITR &root,
                                    _qgmOptiCommand *node )
   {
      PD_TRACE_ENTRY( SDB__QGMBUILDER__BUILDDROPCL ) ;
      INT32 rc = SDB_OK ;
      SDB_ASSERT( 0 == root->children.size(), "impossible" ) ;
      node->_commandType = SQL_GRAMMAR::DROPCL ;
      rc = _table->getAttr( root,
                            node->_fullName ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB__QGMBUILDER__BUILDDROPCL, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMBUILDER__BUILDDROPINX, "_qgmBuilder::_buildDropIndex" )
   INT32 _qgmBuilder::_buildDropIndex( const SQL_CON_ITR &root,
                                      _qgmOptiCommand *node )
   {
      PD_TRACE_ENTRY( SDB__QGMBUILDER__BUILDDROPINX ) ;
      INT32 rc = SDB_OK ;

      SDB_ASSERT( 2 == root->children.size(), "impossible" ) ;
      node->_commandType = SQL_GRAMMAR::DROPINDEX ;
      SQL_CON_ITR itr = root->children.begin() ;
      rc = _table->getField( itr, node->_indexName ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      rc = _table->getAttr( ++itr, node->_fullName ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB__QGMBUILDER__BUILDDROPINX, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMBUILDER__BUILDCRTINX, "_qgmBuilder::_buildCrtIndex" )
   INT32 _qgmBuilder::_buildCrtIndex( const SQL_CON_ITR &root,
                                      _qgmOptiCommand *node )
   {
      PD_TRACE_ENTRY( SDB__QGMBUILDER__BUILDCRTINX ) ;
      INT32 rc = SDB_OK ;

      SDB_ASSERT( 3 == root->children.size()
                  || 4 == root->children.size(), "impossible" ) ;
      node->_commandType = SQL_GRAMMAR::CRTINDEX ;

      SQL_CON_ITR itr = root->children.begin() ;

      if ( 4== root->children.size() )
      {
         node->_uniqIndex = TRUE ;
         ++itr ;
      }

      rc = _table->getField( itr, node->_indexName ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      rc = _table->getAttr( ++itr, node->_fullName ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      rc = _buildIndexColumns( ++itr, node->_indexColumns ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

   done:
      PD_TRACE_EXITRC( SDB__QGMBUILDER__BUILDCRTINX, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMBUILDER__BUILDINXCOLUMNS, "_qgmBuilder::_buildIndexColumns" )
   INT32 _qgmBuilder::_buildIndexColumns( const SQL_CON_ITR &root,
                                          qgmOPFieldVec &columns )
   {
      PD_TRACE_ENTRY( SDB__QGMBUILDER__BUILDINXCOLUMNS ) ;
      INT32 rc = SDB_OK ;
      INT32 type = (INT32)(root->value.id().to_long()) ;
      if ( SQL_GRAMMAR::COMMA == type )
      {
         SDB_ASSERT( 2 == root->children.size(), "impossible" ) ;
         rc = _buildIndexColumns( root->children.begin(),
                                  columns ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }

         rc = _buildIndexColumns( root->children.begin() + 1 ,
                                  columns ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
      else if ( SQL_GRAMMAR::DBATTR == type )
      {
         SDB_ASSERT( root->children.empty(), "impossible" );
         qgmOpField f ;
         f.type = SQL_GRAMMAR::ASC ;
         rc = _table->getField( root,f.value.attr() ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         columns.push_back( f ) ;
      }
      else if ( SQL_GRAMMAR::ASC == type
                || SQL_GRAMMAR::DESC == type )
      {
         SDB_ASSERT( 1 == root->children.size(), "impossible" ) ;
         qgmOpField f ;
         f.type = type ;
         rc = _table->getField( root->children.begin(),f.value.attr() ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         columns.push_back( f ) ;
      }
      else
      {
         PD_LOG( PDERROR, "invalid type:%d", type ) ;
         rc = SDB_SYS ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB__QGMBUILDER__BUILDINXCOLUMNS, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMBUILDER__BUILDCRTCL, "_qgmBuilder::_buildCrtCL" )
   INT32 _qgmBuilder::_buildCrtCL( const SQL_CON_ITR &root,
                                   _qgmOptiCommand *node )
   {
      PD_TRACE_ENTRY( SDB__QGMBUILDER__BUILDCRTCL ) ;
      INT32 rc = SDB_OK ;
      SDB_ASSERT( 0 == root->children.size()
                  || 2 == root->children.size(), "impossible" ) ;
      node->_commandType = SQL_GRAMMAR::CRTCL ;
      if ( 0 == root->children.size() )
      {
         rc = _table->getAttr( root, node->_fullName ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
      else
      {
         SQL_CON_ITR itr = root->children.begin() ;
         rc = _table->getAttr( itr, node->_fullName ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }

         rc = _addOrderBy( ++itr, node->_partition ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
   done:
      PD_TRACE_EXITRC( SDB__QGMBUILDER__BUILDCRTCL, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMBUILDER__BUILDCRTCS, "_qgmBuilder::_buildCrtCS" )
   INT32 _qgmBuilder::_buildCrtCS( const SQL_CON_ITR &root,
                                   _qgmOptiCommand *node )
   {
      PD_TRACE_ENTRY( SDB__QGMBUILDER__BUILDCRTCS ) ;
      INT32 rc = SDB_OK ;
      SDB_ASSERT( 0 == root->children.size(), "impossible" ) ;
      node->_commandType = SQL_GRAMMAR::CRTCS ;
      rc = _table->getAttr( root, node->_fullName ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

   done:
      PD_TRACE_EXITRC( SDB__QGMBUILDER__BUILDCRTCS, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMBUILDER__BUILDDROPCS, "_qgmBuilder::_buildDropCS" )
   INT32 _qgmBuilder::_buildDropCS( const SQL_CON_ITR &root,
                                    _qgmOptiCommand *node )
   {
      PD_TRACE_ENTRY( SDB__QGMBUILDER__BUILDDROPCS ) ;
      INT32 rc = SDB_OK ;
      SDB_ASSERT( 0 == root->children.size(), "impossible" ) ;
      node->_commandType = SQL_GRAMMAR::DROPCS ;
      rc = _table->getAttr( root, node->_fullName ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

   done:
      PD_TRACE_EXITRC( SDB__QGMBUILDER__BUILDDROPCS, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMBUILDER__BUILDSELECT, "_qgmBuilder::_buildSelect" )
   INT32 _qgmBuilder::_buildSelect( const SQL_CON_ITR &root,
                                    _qgmOptiSelect *node,
                                    const CHAR *alias,
                                    UINT32 len )
   {
      PD_TRACE_ENTRY( SDB__QGMBUILDER__BUILDSELECT ) ;
      INT32 rc = SDB_OK ;
      QGM_ALIAS_ASSERT( alias, len )

      SQL_CON_ITR itr = root->children.begin() ;
      if ( root->children.size() < 2 )
      {
         PD_LOG( PDERROR, "invalid children size:%d",
                 root->children.size() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      rc = _addSelector( itr, node ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      rc = _addFrom( ++itr, node ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      ++itr ;
      for ( ; itr != root->children.end(); itr++ )
      {
         INT32 type = (INT32)(itr->value.id().to_long()) ;
         if ( SQL_GRAMMAR::WHERE == type )
         {
            rc = _buildCondition( itr, node->_condition ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
         }
         else if ( SQL_GRAMMAR::GROUPBY == type )
         {
            rc = _addGroupBy( itr, node ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
         }
         else if ( SQL_GRAMMAR::ORDERBY == type )
         {
            rc = _addOrderBy( itr, node->_orderby ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
         }
         else if ( SQL_GRAMMAR::LIMIT == type )
         {
            SQL_CON_ITR value = itr->children.begin() ;
            const CHAR *begin = QGM_VALUEPTR( value ) ;
            UINT32 size = QGM_VALUESIZE( value ) ;
            string str( begin, size ) ;
            node->_limit = ossAtoi( str.c_str() ) ;
         }
         else if ( SQL_GRAMMAR::OFFSET == type )
         {
            SQL_CON_ITR value = itr->children.begin() ;
            const CHAR *begin = QGM_VALUEPTR( value ) ;
            UINT32 size = QGM_VALUESIZE( value ) ;
            string str( begin, size ) ;
            node->_skip = ossAtoi( str.c_str() ) ;
         }
         else if ( SQL_GRAMMAR::SPLITBY == type )
         {
            rc = _addSplitBy( itr, node ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
         }
         else if ( SQL_GRAMMAR::HINT == type )
         {
            rc = _addHint( itr, node ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
         }
         else
         {
            PD_LOG( PDERROR, "err type:%d", type ) ;
            rc = SDB_SYS ;
            goto error ;
         }
      }

      if ( NULL != alias )
      {
         rc = _table->getField( alias, len, node->_alias ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
   done:
      PD_TRACE_EXITRC( SDB__QGMBUILDER__BUILDSELECT, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMBUILDER__BUILDINSERT, "_qgmBuilder::_buildInsert" )
   INT32 _qgmBuilder::_buildInsert( const SQL_CON_ITR &root,
                                    _qgmOptiInsert *insert )
   {
      PD_TRACE_ENTRY( SDB__QGMBUILDER__BUILDINSERT ) ;
      INT32 rc = SDB_OK ;
      SQL_CON_ITR itr = root->children.begin() ;
      INT32 type = SQL_GRAMMAR::SQLMAX ;

      if ( root->children.size() < 2 )
      {
         PD_LOG( PDERROR, "invalid children size:%d",
                 root->children.size() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      type = (INT32)(itr->value.id().to_long()) ;
      if ( SQL_GRAMMAR::DBATTR != type )
      {
         PD_LOG( PDERROR, "invalid type:%d", type ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      rc = _table->getAttr( itr, insert->_collection.value ) ;

      ++itr ;
      type = (INT32)(itr->value.id().to_long()) ;
      if ( SQL_GRAMMAR::SELECT == type )
      {
         _qgmOptiSelect *select = SDB_OSS_NEW
                                  _qgmOptiSelect( _table, _param ) ;
         if ( NULL == select )
         {
            PD_LOG( PDERROR, "failed to allocate mem" ) ;
            rc = SDB_OOM ;
            goto error ;
         }

         insert->_children.push_back( select ) ;
         rc = _buildSelect( itr, select ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
      else
      {
         if ( 3 != root->children.size() )
         {
            PD_LOG( PDERROR, "invalid children size:%d",
                    root->children.size() ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }

         rc = _addColumns( itr, insert ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }

         ++itr ;
         rc = _addValues( itr, insert ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
   done:
      PD_TRACE_EXITRC( SDB__QGMBUILDER__BUILDINSERT, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMBUILDER__ADDSELECTOR, "_qgmBuilder::_addSelector" )
   INT32 _qgmBuilder::_addSelector( const SQL_CON_ITR &root,
                                    _qgmOptiSelect *node,
                                    const CHAR *alias,
                                    UINT32 len )
   {
      PD_TRACE_ENTRY( SDB__QGMBUILDER__ADDSELECTOR ) ;
      INT32 rc = SDB_OK ;
      QGM_ALIAS_ASSERT( alias, len )
      INT32 type = (INT32)(root->value.id().to_long()) ;

      if ( SQL_GRAMMAR::WILDCARD == type )
      {
         qgmOpField field ;
         field.type = type ;
         node->_selector.push_back( field ) ;
      }
      else if ( SQL_GRAMMAR::DBATTR == type )
      {
         qgmOpField attr ;
         attr.type = type ;
         rc = _table->getAttr( root, attr.value ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         if ( NULL != alias )
         {
            rc = _table->getField( alias, len, attr.alias ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
         }

         node->_selector.push_back( attr ) ;
      }
      else if ( SQL_GRAMMAR::COMMA == type )
      {
         PD_CHECK( 2 == root->children.size(),
                   SDB_INVALIDARG,
                   error, PDERROR,
                   "invalid children size:%d",
                   root->children.size() ) ;
         rc = _addSelector( root->children.begin(),
                           node ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         rc = _addSelector( root->children.begin() + 1,
                            node ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
      else if ( SQL_GRAMMAR::AS == type )
      {
         PD_CHECK( 2 == root->children.size(),
                   SDB_INVALIDARG,
                   error, PDERROR,
                   "invalid children size:%d",
                   root->children.size() ) ;
         SQL_CON_ITR itr = root->children.begin() ;
         const CHAR *alias = QGM_VALUEPTR( itr + 1) ;
         UINT32 len = QGM_VALUESIZE( itr + 1 ) ;
         rc = _addSelector( itr, node, alias, len ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
      else if ( SQL_GRAMMAR::FUNC == type )
      {
         qgmOpField func ;
         func.type = type ;

         rc = _table->getField( root, func.value.attr() ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         if ( NULL != alias )
         {
            rc = _table->getField( alias, len, func.alias ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
         }

         if ( NULL != alias )
         {
            rc = _table->getField( alias, len, func.alias ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
         }
         node->_selector.push_back( func ) ;
         node->_hasFunc = TRUE ;
      }
      else
      {
         PD_LOG( PDERROR, "invalid type:%d", type ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB__QGMBUILDER__ADDSELECTOR, rc ) ;
      return rc ;
   error:
      PD_LOG( PDDEBUG, "failed to add selector, type:%d", type ) ;
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMBUILDER__ADDFROM, "_qgmBuilder::_addFrom" )
   INT32 _qgmBuilder::_addFrom( const SQL_CON_ITR &root,
                                _qgmOptiSelect *node,
                                const CHAR *alias,
                                UINT32 len )
   {
      PD_TRACE_ENTRY( SDB__QGMBUILDER__ADDFROM ) ;
      INT32 rc = SDB_OK ;
      QGM_ALIAS_ASSERT( alias, len )
      INT32 type = (INT32)(root->value.id().to_long()) ;

      if ( SQL_GRAMMAR::FROM == type )
      {
         PD_CHECK( 1 == root->children.size(),
                   SDB_INVALIDARG,
                   error, PDERROR,
                   "invalid children size:%d",
                   root->children.size() ) ;
         rc =  _addFrom( root->children.begin(), node ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
      else if ( SQL_GRAMMAR::DBATTR == type )
      {
         node->_collection.type = type ;
         rc = _table->getAttr( root, node->_collection.value ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         if ( NULL != alias )
         {
            rc = _table->getField( alias, len, node->_collection.alias ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
            rc = _table->getField( alias, len, node->_alias ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
         }
      }
      else if ( SQL_GRAMMAR::AS == type )
      {
         PD_CHECK( 2 == root->children.size(),
                   SDB_INVALIDARG,
                   error, PDERROR,
                   "invalid children size:%d",
                   root->children.size() ) ;

         SQL_CON_ITR itr = root->children.begin() ;
         const CHAR *alias = QGM_VALUEPTR( itr + 1) ;
         UINT32 len = QGM_VALUESIZE( itr + 1 ) ;
         rc = _addFrom( itr, node, alias, len ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
      else if ( isJoin( type ) )
      {
         PD_CHECK( 2 <= root->children.size(),
                   SDB_INVALIDARG,
                   error, PDERROR,
                   "invalid children size:%d",
                   root->children.size() ) ;

         {
         SQL_CON_ITR itr = root->children.begin() ;
         _qgmOptiNLJoin *join = NULL ;
         join = SDB_OSS_NEW _qgmOptiNLJoin( type, _table, _param ) ;
         if ( NULL == join )
         {
            PD_LOG( PDERROR, "failed to allocate mem") ;
            rc = SDB_OOM ;
            goto error ;
         }

         node->_children.push_back( join ) ;

         rc = _buildJoin( itr, join ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }

         rc = _buildJoin( ++itr, join ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }

         if ( NULL != alias )
         {
            rc = _table->getField( alias, len, join->_alias ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
         }

         if ( ++itr != root->children.end() )
         {
            rc = _buildCondition( itr, join->_condition ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
         }
         }
      }
      else if ( SQL_GRAMMAR::SELECT == type )
      {
         _qgmOptiSelect *select = SDB_OSS_NEW
                                  _qgmOptiSelect( _table, _param ) ;
         if ( NULL == select )
         {
            PD_LOG( PDERROR, "failed to allocate mem") ;
            rc = SDB_OOM ;
            goto error ;
         }

         node->_children.push_back( select ) ;

         rc = _buildSelect( root, select, alias, len ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }

      }
      else
      {
         PD_LOG( PDERROR, "unknown type%d", type ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB__QGMBUILDER__ADDFROM, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMBUILDER__BUILDJOIN, "_qgmBuilder::_buildJoin" )
   INT32 _qgmBuilder::_buildJoin( const SQL_CON_ITR &root,
                                  _qgmOptiNLJoin *node,
                                  const CHAR *alias,
                                  UINT32 len )
   {
      PD_TRACE_ENTRY( SDB__QGMBUILDER__BUILDJOIN ) ;
      INT32 rc = SDB_OK ;
      INT32 type = (INT32)(root->value.id().to_long()) ;

      if ( SQL_GRAMMAR::DBATTR == type )
      {
         _qgmOptiSelect *select = SDB_OSS_NEW
                                  _qgmOptiSelect( _table, _param ) ;
         if ( NULL == select )
         {
            PD_LOG( PDERROR, "failed to allocate mem") ;
            rc = SDB_OOM ;
            goto error ;
         }

         rc = _table->getAttr( root, select->_collection.value ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }

         {
         _qgmOpField wildcard ;
         wildcard.type = SQL_GRAMMAR::WILDCARD ;
         select->_selector.push_back( wildcard ) ;
         }

         if ( NULL != alias )
         {
            rc = _table->getField( alias, len, select->_alias ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
         }

         node->_children.push_back( select ) ;
      }
      else if ( SQL_GRAMMAR::AS == type )
      {
         PD_CHECK( 2 == root->children.size(),
                   SDB_INVALIDARG,
                   error, PDERROR,
                   "invalid children size:%d",
                   root->children.size() ) ;
         SQL_CON_ITR itr = root->children.begin() ;
         const CHAR *alias = QGM_VALUEPTR( itr + 1) ;
         UINT32 len = QGM_VALUESIZE( itr + 1 ) ;
         rc = _buildJoin( itr, node, alias, len ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
      else if ( SQL_GRAMMAR::SELECT == type )
      {
         _qgmOptiSelect *select = SDB_OSS_NEW
                                  _qgmOptiSelect( _table, _param ) ;
         if ( NULL == select )
         {
            PD_LOG( PDERROR, "failed to allocate mem") ;
            rc = SDB_OOM ;
            goto error ;
         }

         node->_children.push_back( select ) ;

         rc = _buildSelect( root, select, alias, len ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }

         if ( NULL != alias )
         {
            rc = _table->getField( alias, len, select->_alias ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
         }

      }
      else
      {
         PD_LOG( PDERROR, "unknown type%d", type ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

   done:
      PD_TRACE_EXITRC( SDB__QGMBUILDER__BUILDJOIN, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   static INT32 buildInArray( const SQL_CON_ITR &root,
                              BSONObjBuilder &builder )
   {
      INT32 rc = SDB_OK ;
      INT32 type = (INT32)(root->value.id().to_long()) ;
      if ( SQL_GRAMMAR::DIGITAL == type )
      {
         std::string strNumber( root->value.begin(),
                                root->value.end() ) ;
         builder.appendAsNumber( "", strNumber ) ;
      }
      else if ( SQL_GRAMMAR::STR == type )
      {
         std::string str( root->value.begin(),
                          root->value.end() ) ;
         builder.append( "", str ) ;
      }
      else if ( SQL_GRAMMAR::COMMA == type )
      {
         SDB_ASSERT( 2 == root->children.size(), "impossible" ) ;
         rc = buildInArray( root->children.begin(),
                            builder ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }

         rc = buildInArray( root->children.begin()+1,
                            builder ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
      else
      {
         PD_LOG( PDERROR, "invalid node was found when build condition:%d",
                          type ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMBUILDER__BUILDINCONDITION, "_qgmBuilder::_buildInCondition" )
   INT32 _qgmBuilder::_buildInCondition( const SQL_CON_ITR &root,
                                         _qgmConditionNode *&condition )
   {
      PD_TRACE_ENTRY( SDB__QGMBUILDER__BUILDINCONDITION ) ;
      INT32 rc = SDB_OK ;
      BSONObjBuilder builder1, builder2 ;
      BSONArrayBuilder arrBuilder ;
      BSONObj obj ;
      rc = buildInArray( root, builder1 );
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to build array from in" ) ;
         goto error ;
      }

      {
      obj = builder1.obj() ;
      BSONObjIterator itr( obj ) ;
      while ( itr.more() )
      {
         arrBuilder.append( itr.next() ) ;
      }

      builder2.append( "in", arrBuilder.arr() ) ;

      condition = SDB_OSS_NEW _qgmConditionNode( SQL_GRAMMAR::SQLMAX + 1 ) ;
      if ( NULL == condition )
      {
         PD_LOG( PDERROR, "failed to allocate mem." ) ;
         rc = SDB_OOM ;
         goto error ;
      }

      rc = _param->addConst( builder2.obj(), condition->var ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
      }
   done:
      PD_TRACE_EXITRC( SDB__QGMBUILDER__BUILDINCONDITION, rc ) ;
      return rc ;
   error:
      if ( NULL != condition )
      {
         SAFE_OSS_DELETE( condition ) ;
      }
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMBUILDER__BUILDCONDITION, "_qgmBuilder::_buildCondition" )
   INT32 _qgmBuilder::_buildCondition( const SQL_CON_ITR &root,
                                       _qgmConditionNode *&condition )
   {
      PD_TRACE_ENTRY( SDB__QGMBUILDER__BUILDCONDITION ) ;
      INT32 rc = SDB_OK ;
      INT32 type = (INT32)(root->value.id().to_long()) ;

      if ( SQL_GRAMMAR::ON == type || SQL_GRAMMAR::WHERE == type )
      {
         PD_CHECK( 1 == root->children.size(),
                   SDB_INVALIDARG,
                   error, PDERROR,
                   "invalid children size:%d",
                   root->children.size() ) ;

         rc = _buildCondition( root->children.begin(),
                               condition ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
      else if ( SQL_GRAMMAR::EG == type
                || SQL_GRAMMAR::NE == type
                || SQL_GRAMMAR::GT == type
                || SQL_GRAMMAR::LT == type
                || SQL_GRAMMAR::GTE == type
                || SQL_GRAMMAR::LTE == type
                || SQL_GRAMMAR::AND == type
                || SQL_GRAMMAR::OR == type
                || SQL_GRAMMAR::LIKE == type
                || SQL_GRAMMAR::INN == type
                || SQL_GRAMMAR::IS == type )
      {
         PD_CHECK( 2 == root->children.size(),
                   SDB_INVALIDARG,
                   error, PDERROR,
                   "invalid children size:%d",
                   root->children.size() ) ;
         condition = SDB_OSS_NEW qgmConditionNode( type ) ;
         if ( NULL == condition )
         {
            PD_LOG( PDERROR, "failed to allocate mem" ) ;
            rc = SDB_OOM ;
            goto error ;
         }

         rc = _buildCondition( root->children.begin(),
                               condition->left ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }

         if ( SQL_GRAMMAR::INN == type )
         {
            BSONArrayBuilder inBuilder ;
            rc = _buildInCondition( root->children.begin()+1,
                                    condition->right );
            if ( SDB_OK != rc )
            {
               goto error ;
            }
         }
         else
         {
            rc = _buildCondition( root->children.begin()+1,
                                  condition->right ) ;
            if ( SDB_OK != rc )
            {
               goto error ;
            }
         }
      }
      else if ( SQL_GRAMMAR::NOT == type )
      {
         PD_CHECK( 1 == root->children.size(),
                   SDB_INVALIDARG,
                   error, PDERROR,
                   "invalid children size:%d",
                   root->children.size() ) ;
         condition = SDB_OSS_NEW qgmConditionNode( type ) ;
         if ( NULL == condition )
         {
            PD_LOG( PDERROR, "failed to allocate mem" ) ;
            rc = SDB_OOM ;
            goto error ;
         }
         rc = _buildCondition( root->children.begin(),
                               condition->left);
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
      else if ( SQL_GRAMMAR::DIGITAL == type
                || SQL_GRAMMAR::STR == type )
      {
         condition = SDB_OSS_NEW qgmConditionNode( type ) ;
         if ( NULL == condition )
         {
            PD_LOG( PDERROR, "failed to allocate mem" ) ;
            rc = SDB_OOM ;
            goto error ;
         }
         rc = _table->getField( root, condition->value.attr() ) ;
         condition->type = type ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
      else if ( SQL_GRAMMAR::DBATTR == type )
      {
         condition = SDB_OSS_NEW qgmConditionNode( type ) ;
         if ( NULL == condition )
         {
            PD_LOG( PDERROR, "failed to allocate mem" ) ;
            rc = SDB_OOM ;
            goto error ;
         }
         rc = _table->getAttr( root, condition->value ) ;
         condition->type = type ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
      else if ( SQL_GRAMMAR::DATE == type )
      {
         SDB_ASSERT( 1 == root->children.size(), "impossible" ) ;
         _qgmOpField f ;
         condition = SDB_OSS_NEW qgmConditionNode( type ) ;
         if ( NULL == condition )
         {
            PD_LOG( PDERROR, "failed to allocate mem" ) ;
            rc = SDB_OOM ;
            goto error ;
         }
         condition->type = SQL_GRAMMAR::SQLMAX + 1 ;

         rc = _table->getField( root->children.begin(),
                                f.value.attr() ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         f.type = SQL_GRAMMAR::DATE ;

         rc = _param->addConst( f, condition->var ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
      else if ( SQL_GRAMMAR::NULLL == type )
      {
         condition = SDB_OSS_NEW qgmConditionNode( type ) ;
         if ( NULL == condition )
         {
            PD_LOG( PDERROR, "failed to allocate mem" ) ;
            rc = SDB_OOM ;
            goto error ;
         }
      }
      else
      {
         PD_LOG( PDERROR, "invalid type:%d",type ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB__QGMBUILDER__BUILDCONDITION, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMBUILDER__ADDSPLITBY, "_qgmBuilder::_addSplitBy" )
   INT32 _qgmBuilder::_addSplitBy( const SQL_CON_ITR &root,
                                   _qgmOptiSelect *node )
   {
      PD_TRACE_ENTRY( SDB__QGMBUILDER__ADDSPLITBY ) ;
      INT32 rc = SDB_OK ;
      INT32 type = (INT32)(root->value.id().to_long()) ;
      SDB_ASSERT( SQL_GRAMMAR::SPLITBY == type &&
                  1 == root->children.size(), "impossible" ) ;
      rc = _table->getAttr( root->children.begin(),
                            node->_splitby ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB__QGMBUILDER__ADDSPLITBY, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMBUILDER__ADDGROUPBY, "_qgmBuilder::_addGroupBy" )
   INT32 _qgmBuilder::_addGroupBy( const SQL_CON_ITR &root,
                                   _qgmOptiSelect *node )
   {
      PD_TRACE_ENTRY( SDB__QGMBUILDER__ADDGROUPBY ) ;
      INT32 rc = SDB_OK ;
      INT32 type = (INT32)(root->value.id().to_long()) ;
      if ( SQL_GRAMMAR::GROUPBY == type )
      {
         PD_CHECK( 1 == root->children.size(),
                   SDB_INVALIDARG,
                   error, PDERROR,
                   "invalid children size:%d",
                   root->children.size() ) ;

         rc = _addGroupBy( root->children.begin(),
                           node ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
      else if ( SQL_GRAMMAR::DBATTR == type )
      {
         qgmOpField field  ;
         rc = _table->getAttr( root, field.value ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         field.type = SQL_GRAMMAR::ASC ;
         if ( !isFromOne( field, node->_groupby, FALSE ) )
         {
            node->_groupby.push_back( field ) ;
         }
      }
      else if ( SQL_GRAMMAR::COMMA == type )
      {
         PD_CHECK( 2 == root->children.size(),
                   SDB_INVALIDARG,
                   error, PDERROR,
                   "invalid children size:%d",
                   root->children.size() ) ;
         rc = _addGroupBy( root->children.begin(),
                           node ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         rc = _addGroupBy( root->children.begin()+1,
                           node ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
      else if ( SQL_GRAMMAR::DESC == type ||
                SQL_GRAMMAR::ASC == type )
      {
         PD_CHECK( 1 == root->children.size(),
                   SDB_INVALIDARG,
                   error, PDERROR,
                   "invalid children size:%d",
                   root->children.size() ) ;

         {
         SQL_CON_ITR itr = root->children.begin() ;
         SDB_ASSERT( SQL_GRAMMAR::DBATTR
                     == (INT32)(itr->value.id().to_long()),
                     "impossible" ) ;
         qgmOpField field  ;
         field.type = type ;
         rc = _table->getAttr( itr, field.value ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         if ( !isFromOne( field, node->_groupby, FALSE ) )
         {
            node->_groupby.push_back( field ) ;
         }
         }
      }
      else
      {
         PD_LOG( PDERROR, "invalid type:%d", type ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB__QGMBUILDER__ADDGROUPBY, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMBUILDER__ADDORDERBY, "_qgmBuilder::_addOrderBy" )
   INT32 _qgmBuilder::_addOrderBy( const SQL_CON_ITR &root,
                                   qgmOPFieldVec &order )
   {
      PD_TRACE_ENTRY( SDB__QGMBUILDER__ADDORDERBY ) ;
      INT32 rc = SDB_OK ;
      INT32 type = (INT32)(root->value.id().to_long()) ;
      if ( SQL_GRAMMAR::ORDERBY == type )
      {
         PD_CHECK( 1 == root->children.size(),
                   SDB_INVALIDARG,
                   error, PDERROR,
                   "invalid children size:%d",
                   root->children.size() ) ;

         rc = _addOrderBy( root->children.begin(),
                           order ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
      else if ( SQL_GRAMMAR::DBATTR == type )
      {
         qgmOpField field  ;
         rc = _table->getAttr( root, field.value ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }

         field.type = SQL_GRAMMAR::ASC ;
         if ( !isFromOne( field, order, FALSE ) )
         {
            order.push_back( field ) ;
         }
      }
      else if ( SQL_GRAMMAR::COMMA == type )
      {
         PD_CHECK( 2 == root->children.size(),
                   SDB_INVALIDARG,
                   error, PDERROR,
                   "invalid children size:%d",
                   root->children.size() ) ;
         rc = _addOrderBy( root->children.begin(),
                           order ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         rc = _addOrderBy( root->children.begin()+1,
                           order ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
      else if ( SQL_GRAMMAR::DESC == type
                || SQL_GRAMMAR::ASC == type )
      {
         PD_CHECK( 1 == root->children.size(),
                   SDB_INVALIDARG,
                   error, PDERROR,
                   "invalid children size:%d",
                   root->children.size() ) ;

         {
         SQL_CON_ITR itr = root->children.begin() ;
         SDB_ASSERT( SQL_GRAMMAR::DBATTR
                     == (INT32)(itr->value.id().to_long()),
                     "impossible" ) ;
         qgmOpField field  ;
         field.type = type ;
         rc = _table->getAttr( itr, field.value ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         if ( !isFromOne( field, order, FALSE ) )
         {
            order.push_back( field ) ;
         }
         }
      }
      else
      {
         PD_LOG( PDERROR, "invalid type:%d", type ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB__QGMBUILDER__ADDORDERBY, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGBUILDER__ADDLIMIT, "_qgmBuilder::_addLimit" )
   INT32 _qgmBuilder::_addLimit( const SQL_CON_ITR &root,
                                 _qgmOptiSelect *node )
   {
      PD_TRACE_ENTRY( SDB__QGBUILDER__ADDLIMIT ) ;
      INT32 rc = SDB_OK ;
      INT32 type = (INT32)(root->value.id().to_long()) ;
      if ( SQL_GRAMMAR::LIMIT == type )
      {
         PD_CHECK( 1 == root->children.size(),
                   SDB_INVALIDARG,
                   error, PDERROR,
                   "invalid children size:%d",
                   root->children.size() ) ;
         rc = _addLimit( root->children.begin(),
                         node ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
      else if ( SQL_GRAMMAR::DIGITAL == type )
      {
         CHAR *tail = (CHAR *)(QGM_VALUEPTR(root) + QGM_VALUESIZE(root));
         CHAR tmp = *tail ;
         *tail = '\0' ;
         node->_limit = ossAtoll( QGM_VALUEPTR(root) ) ;
         *tail = tmp ;
      }
      else
      {
         PD_LOG( PDERROR, "invalid type:%d", type ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB__QGBUILDER__ADDLIMIT, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMBUILDER__ADDSKIP, "_qgmBuilder::_addSkip" )
   INT32 _qgmBuilder::_addSkip( const SQL_CON_ITR &root,
                                _qgmOptiSelect *node )
   {
      PD_TRACE_ENTRY( SDB__QGMBUILDER__ADDSKIP ) ;
      INT32 rc = SDB_OK ;
      INT32 type = (INT32)(root->value.id().to_long()) ;
      if ( SQL_GRAMMAR::OFFSET == type )
      {
         PD_CHECK( 1 == root->children.size(),
                   SDB_INVALIDARG,
                   error, PDERROR,
                   "invalid children size:%d",
                   root->children.size() ) ;
         rc = _addSkip( root->children.begin(),
                        node ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
      else if ( SQL_GRAMMAR::DIGITAL == type )
      {
         CHAR *tail = (CHAR *)(QGM_VALUEPTR(root) + QGM_VALUESIZE(root));
         CHAR tmp = *tail ;
         *tail = '\0' ;
         node->_limit = ossAtoll( QGM_VALUEPTR(root) ) ;
         *tail = tmp ;
      }
      else
      {
         PD_LOG( PDERROR, "invalid type:%d", type ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB__QGMBUILDER__ADDSKIP, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMBUILDER__ADDCOLUMNS, "_qgmBuilder::_addColumns" )
   INT32 _qgmBuilder::_addColumns( const SQL_CON_ITR &root,
                                   _qgmOptiInsert *node )
   {
      PD_TRACE_ENTRY( SDB__QGMBUILDER__ADDCOLUMNS ) ;
      INT32 rc = SDB_OK ;
      INT32 type = (INT32)(root->value.id().to_long()) ;
      if ( SQL_GRAMMAR::DBATTR == type )
      {
         qgmOpField field ;
         field.type = type ;
         rc = _table->getAttr( root, field.value ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         node->_columns.push_back( field ) ;
      }
      else if ( SQL_GRAMMAR::COMMA == type )
      {
         PD_CHECK( 2 == root->children.size(),
                   SDB_INVALIDARG,
                   error, PDERROR,
                   "invalid children size:%d",
                   root->children.size() ) ;

         rc = _addColumns( root->children.begin(),
                           node ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }

         rc = _addColumns( root->children.begin() + 1,
                           node ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
      else
      {
         PD_LOG( PDERROR, "invalid type:%d", type ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB__QGMBUILDER__ADDCOLUMNS, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMBUILDER__ADDVALUES, "_qgmBuilder::_addValues" )
   INT32 _qgmBuilder::_addValues( const SQL_CON_ITR &root,
                                  _qgmOptiInsert *node )
   {
      PD_TRACE_ENTRY( SDB__QGMBUILDER__ADDVALUES ) ;
      INT32 rc = SDB_OK ;
      INT32 type = (INT32)(root->value.id().to_long()) ;
      if ( SQL_GRAMMAR::DIGITAL == type
           || SQL_GRAMMAR::STR == type )
      {
         qgmOpField field ;
         field.type = type ;
         rc = _table->getField( root, field.value.attr() ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         node->_values.push_back( field ) ;
      }
      else if ( SQL_GRAMMAR::COMMA == type )
      {
         PD_CHECK( 2 == root->children.size(),
                   SDB_INVALIDARG,
                   error, PDERROR,
                   "invalid children size:%d",
                   root->children.size() ) ;

         rc = _addValues( root->children.begin(),
                           node ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }

         rc = _addValues( root->children.begin() + 1,
                           node ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }
      else if ( SQL_GRAMMAR::DATE == type )
      {
         SDB_ASSERT( 1 == root->children.size(), "impossible" ) ;
         qgmOpField field ;
         field.type = type ;
         rc = _table->getField( root->children.begin(),
                                field.value.attr() ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
         node->_values.push_back( field ) ;
      }
      else
      {
         PD_LOG( PDERROR, "invalid type:%d", type ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB__QGMBUILDER__ADDVALUES, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION( SDB__QGMBUILDER__ADDHINT, "_qgmBuilder::_addHint" )
   INT32 _qgmBuilder::_addHint( const SQL_CON_ITR &root,
                                _qgmOptiSelect *node )
   {
      PD_TRACE_ENTRY( SDB__QGMBUILDER__ADDHINT ) ;
      INT32 rc = SDB_OK ;
      SQL_CON_ITR itr = root->children.begin() ;
      for ( ; itr != root->children.end(); itr++ )
      {
         INT32 type = (INT32)(itr->value.id().to_long()) ;
         if ( SQL_GRAMMAR::FUNC == type )
         {
            _qgmHint hint ;
            rc = qgmFindFieldFromFunc( QGM_VALUEPTR( itr),
                                       QGM_VALUESIZE( itr ),
                                       hint.value,
                                       hint.param,
                                       _table,
                                       FALSE ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "failed to parse hint from itr:%d", rc ) ;
               goto error ;
            }

            node->_hints.push_back( hint ) ;
         }
      }
   done:
      PD_TRACE_EXITRC( SDB__QGMBUILDER__ADDHINT, rc ) ;
      return rc ;
   error:
      goto done ;
   }

}

