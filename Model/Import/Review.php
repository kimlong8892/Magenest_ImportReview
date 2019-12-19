<?php

namespace Magenest\ImportReview\Model\Import;

use Exception;
use Magento\Catalog\Api\CategoryRepositoryInterface;
use Magento\Catalog\Model\CategoryFactory;
use Magento\Framework\App\ResourceConnection;
use Magento\Framework\DB\Adapter\AdapterInterface;
use Magento\Framework\Json\Helper\Data as JsonHelper;
use Magento\ImportExport\Helper\Data as ImportHelper;
use Magento\ImportExport\Model\Import;
use Magento\ImportExport\Model\Import\Entity\AbstractEntity;
use Magento\ImportExport\Model\Import\ErrorProcessing\ProcessingErrorAggregatorInterface;
use Magento\ImportExport\Model\ResourceModel\Helper;
use Magento\ImportExport\Model\ResourceModel\Import\Data;

class Review extends AbstractEntity
{
    const ENTITY_CODE = 'import_review';
    const TABLE = 'review_detail';
    const ENTITY_ID_COLUMN = 'review_id';


    const NICK_NAME = "nickname";
    const DATE = "created_at";
    const TITLE = "title";
    const DETAILS = "detail";
    const PRODUCT_ID = "product_id";
    const STATUS = "status";
    //const CUSTOMER_ID = "customer_id";
    /**
     * If we should check column names
     */
    protected $needColumnCheck = true;

    /**
     * Need to log in import history
     */
    protected $logInHistory = true;

    /**
     * Permanent entity columns.
     */

    /**
     * Valid column names
     */
    protected $validColumnNames = [
        self::ENTITY_ID_COLUMN,
        self::NICK_NAME,
        self::DATE,
        self::TITLE,
        self::DETAILS,
        self::PRODUCT_ID,
        self::STATUS,
        //self::CUSTOMER_ID
    ];

    /**
     * @var AdapterInterface
     */
    protected $connection;

    /**
     * @var ResourceConnection
     */
    private $resource;
    protected $modelPreviewFactory;
    protected $resourcePreview;
    protected $previewRepository;

    /**
     * Courses constructor.
     *
     * @param JsonHelper $jsonHelper
     * @param ImportHelper $importExportData
     * @param Data $importData
     * @param ResourceConnection $resource
     * @param Helper $resourceHelper
     * @param ProcessingErrorAggregatorInterface $errorAggregator
     */
    public function __construct(
        JsonHelper $jsonHelper,
        ImportHelper $importExportData,
        Data $importData,
        ResourceConnection $resource,
        Helper $resourceHelper,
        ProcessingErrorAggregatorInterface $errorAggregator,
        \Magento\Review\Model\ReviewFactory $modelPreviewFactory,
        \Magento\Review\Model\ResourceModel\Review $resourcePreview


    ) {
        $this->jsonHelper = $jsonHelper;
        $this->_importExportData = $importExportData;
        $this->_resourceHelper = $resourceHelper;
        $this->_dataSourceModel = $importData;
        $this->resource = $resource;
        $this->connection = $resource->getConnection(ResourceConnection::DEFAULT_CONNECTION);
        $this->errorAggregator = $errorAggregator;
        $this->initMessageTemplates();
        $this->modelPreviewFactory = $modelPreviewFactory;
        $this->resourcePreview = $resourcePreview;

    }

    /**
     * Entity type code getter.
     *
     * @return string
     */
    public function getEntityTypeCode()
    {
        return static::ENTITY_CODE;
    }

    /**
     * Get available columns
     *
     * @return array
     */
    public function getValidColumnNames(): array
    {
        return $this->validColumnNames;
    }

    /**
     * Row validation
     *
     * @param array $rowData
     * @param int $rowNum
     *
     * @return bool
     */
    public function validateRow(array $rowData, $rowNum): bool
    {

        forEach($rowData as $key => $value)
        {
            $required = (string)$value;
            if($required == '')
            {
                $this->addRowError($key.'IsRequired', $rowNum);
            }
        }
        if (isset($this->_validatedRows[$rowNum])) {
            return !$this->getErrorAggregator()->isRowInvalid($rowNum);
        }

        $this->_validatedRows[$rowNum] = true;
        return !$this->getErrorAggregator()->isRowInvalid($rowNum);
    }

    /**
     * Init Error Messages
     */
    private function initMessageTemplates()
    {

        $this->addMessageTemplate(
            'review_idIsRequired',
            __('The review_id cannot be empty.')
        );
        $this->addMessageTemplate(
            'nicknameIsRequired',
            __('The nickname cannot be empty.')
        );
        $this->addMessageTemplate(
            'created_atIsRequired',
            __('The created_at cannot be empty.')
        );
        $this->addMessageTemplate(
            'titleIsRequired',
            __('The title cannot be empty.')
        );
        $this->addMessageTemplate(
            'detailIsRequired',
            __('The detail cannot be empty.')
        );
        $this->addMessageTemplate(
            'product_idIsRequired',
            __('The product_id cannot be empty.')
        );
        $this->addMessageTemplate(
            'statusIsRequired',
            __('The status cannot be empty.')
        );


    }

    /**
     * Import data
     *
     * @return bool
     *
     * @throws Exception
     */
    protected function _importData(): bool
    {
        switch ($this->getBehavior()) {
            case Import::BEHAVIOR_DELETE:
                $this->deleteEntity();
                break;
            case Import::BEHAVIOR_REPLACE:
                $this->saveAndReplaceEntity();
                break;
            case Import::BEHAVIOR_APPEND:
                $this->saveAndReplaceEntity();
                break;
        }
        return true;
    }

    /**
     * Delete entities
     *
     * @return bool
     */
    private function deleteEntity(): bool
    {
        $rows = [];
        while ($bunch = $this->_dataSourceModel->getNextBunch()) {
            foreach ($bunch as $rowNum => $rowData) {
                $this->validateRow($rowData, $rowNum);
                if (!$this->getErrorAggregator()->isRowInvalid($rowNum)) {
                    $rowId = $rowData[static::ENTITY_ID_COLUMN];
                    $rows[] = $rowId;
                }

                if ($this->getErrorAggregator()->hasToBeTerminated()) {
                    $this->getErrorAggregator()->addRowToSkip($rowNum);
                }
            }
        }

        if ($rows) {
            return $this->deleteEntityFinish(array_unique($rows));
        }

        return false;
    }


    /**
     * Save and replace entities
     *
     * @return void
     */
    private function saveAndReplaceEntity()
    {
        $behavior = $this->getBehavior();
        $rows = [];
        while ($bunch = $this->_dataSourceModel->getNextBunch()) {
            $entityList = [];

            foreach ($bunch as $rowNum => $row) {
                if (!$this->validateRow($row, $rowNum)) {
                    continue;
                }

                if ($this->getErrorAggregator()->hasToBeTerminated()) {
                    $this->getErrorAggregator()->addRowToSkip($rowNum);

                    continue;
                }

                $rowId = $row[static::ENTITY_ID_COLUMN];
                $rows[] = $rowId;
                $columnValues = [];

                foreach ($this->getAvailableColumns() as $columnKey) {
                    $columnValues[$columnKey] = $row[$columnKey];
                }

                $entityList[$rowId][] = $columnValues;
            }

            if (Import::BEHAVIOR_REPLACE === $behavior) {
                if ($rows) {
                    $this->saveEntityFinish($entityList, true);
                }
            } elseif (Import::BEHAVIOR_APPEND === $behavior) {
                $this->saveEntityFinish($entityList);
            }
        }
    }

    /**
     * Save entities
     *
     * @param array $entityData
     *
     * @return bool
     */
    private function saveEntityFinish(array $entityData, $replace = false): bool
    {
        if ($entityData) {
            $rows = [];

            foreach ($entityData as $entityRows) {
                foreach ($entityRows as $row) {
                    $rows[] = $row;
                }
            }

            if ($rows) {
                if (!$replace) {
                    $this->AddOrUpdate($rows);
                } else {
                    $this->Replace($rows);
                }
                return true;
            }

            return false;
        }

    }

    private function Replace($rows)
    {
        $countDelete = 0;
        $modelReview = $this->modelPreviewFactory->create();
        $datas = $modelReview->getCollection()->getData();
        $rowId = [];
        foreach ($datas as $data) {
            $rowId[] = $data['review_id'];
        }
        $this->deleteEntityFinish(array_unique($rowId));
        $this->AddOrUpdate($rows);
        $this->countItemsDeleted = count($rowId);
    }

    private function AddOrUpdate($rows)
    {
        $countCreate = 0;
        $countUpdate = 0;
        foreach ($rows as $row) {
            $modelReview = $this->modelPreviewFactory->create();
            if ($modelReview->load($row['review_id'])->getId() != null) {
                $modelReview->load($row['review_id']);
                $countUpdate++;
            } else {
                $countCreate++;
            }
            $modelReview->setStatusId($row[self::STATUS]);
            $modelReview->setEntityPkValue($row[self::PRODUCT_ID]);
            $modelReview->setNickname($row[self::NICK_NAME]);
            $modelReview->setCreatedAt($row[self::DATE]);
            $modelReview->setTitle($row[self::TITLE]);
            $modelReview->setDetail($row[self::DETAILS]);
            $modelReview->setStatusId($row[self::STATUS]);
            $modelReview->setEntityId($row[self::PRODUCT_ID]);
            $modelReview->save();
        }
        $this->countItemsCreated = (int)$countCreate;
        $this->countItemsUpdated = (int)$countUpdate;
    }

    /**
     * Delete entities
     *
     * @param array $entityIds
     *
     * @return bool
     */
    private function deleteEntityFinish(array $entityIds): bool
    {
        if ($entityIds) {
            try {
                $this->countItemsDeleted += $this->connection->delete(
                    $this->connection->getTableName(static::TABLE),
                    $this->connection->quoteInto(static::ENTITY_ID_COLUMN . ' IN (?)', $entityIds)
                );

                return true;
            } catch (Exception $e) {
                return false;
            }
        }

        return false;
    }

    /**
     * Get available columns
     *
     * @return array
     */
    private function getAvailableColumns(): array
    {
        return $this->validColumnNames;
    }
}
