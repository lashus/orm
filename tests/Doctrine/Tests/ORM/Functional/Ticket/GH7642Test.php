<?php

declare(strict_types=1);

namespace Doctrine\Tests\ORM\Functional\Ticket;

use Doctrine\Tests\OrmFunctionalTestCase;

/**
 * @group GH7642
 */
class GH7642Test extends OrmFunctionalTestCase
{

    private $limit;

    /**
     * {@inheritDoc}
     */
    protected function setUp() : void
    {
        parent::setUp();

        $this->_schemaTool->createSchema(
            [
                $this->_em->getClassMetadata(GH7642Entity::class),
                $this->_em->getClassMetadata(GH7642UserEntity::class),
            ]
        );

        $this->limit = 25000;
    }

    private function clearMemory() {
        $this->_em->clear();
        gc_collect_cycles();
    }

    private function getMemoryFootprint() {
        return memory_get_peak_usage(true) / 1000000;
    }

    private function runRawQuery() : void {

        // iterate over results
        $conn = $this->_em->getConnection();
        $repo = $this->_em->getRepository(GH7642Entity::class);
        $query = $repo->createNamedQuery('count_points');
        $sql = $query->getSQL();
        $batchSize = 2000;
        $page = 1;

        do {

            $offset = ($page - 1) * $batchSize;
            $qb = $repo->createQueryBuilder('u');
            $qb->setFirstResult($offset);
            $qb->setMaxResults($batchSize);
            $qb->setCacheable(false);
            $data = $qb->getQuery()->getResult();

            foreach($data as $entity) {

                // bypass named query execution through doctrine for memory saving
                $stmt = $conn->prepare($sql);
                $stmt->execute([$entity->user->id]);

                $stmt->fetch(\PDO::FETCH_COLUMN);
                unset($stmt);

            }

            unset($data);

            $page++;

        } while($offset < $this->limit);

    }

    private function runNamedQuery() : void {

        // iterate over results
        $repo = $this->_em->getRepository(GH7642Entity::class);
        $batchSize = 2000;
        $page = 1;

        do {

            $offset = ($page - 1) * $batchSize;
            $qb = $repo->createQueryBuilder('u');
            $qb->setFirstResult($offset);
            $qb->setMaxResults($batchSize);
            $qb->setCacheable(false);
            $data = $qb->getQuery()->getResult();

            foreach($data as $entity) {

                $query = $repo->createNamedQuery('count_points');
                $query->setParameter('user', $entity->user->id);
                $query->setCacheable(false);

                $query->getSingleScalarResult();

            }

            unset($data);

            $page++;

        } while($offset < $this->limit);

    }

    public function testIssue() : void
    {

        for($i=0;$i<$this->limit;$i++) {
            $userEntity = new GH7642UserEntity();
            $entity = new GH7642Entity($userEntity, mt_rand(0,100));

            $this->_em->persist($entity);
        }

        $this->_em->flush();
        $this->clearMemory();

        $this->runRawQuery();
        $rawQueryMemory = $this->getMemoryFootprint();
        $this->clearMemory();

        $this->runNamedQuery();
        $namedQueryMemory = $this->getMemoryFootprint();

        // lets assume that named query should use 2x more memory at most.
        $this->assertLessThanOrEqual($rawQueryMemory*2, $namedQueryMemory);

    }

}

/**
 * @Entity
 *
 * @NamedQueries({
 *     @NamedQuery(name="count_points", query="SELECT SUM(a.points) FROM __CLASS__ a WHERE a.user = :user GROUP BY a.user")
 * })
 */
class GH7642Entity
{
    /**
     * @Id
     * @Column(type="integer")
     * @GeneratedValue
     */
    public $id;

    /**
     * @var int
     * @Column(type="integer")
     */
    public $points;

    /**
     * @ManyToOne(targetEntity="GH7642UserEntity", inversedBy="stats", cascade={"all"})
     */
    public $user;

    public function __construct($user, $points)
    {
        $this->user = $user;
        $this->points = $points;
    }
}


/**
 * @Entity
 */
class GH7642UserEntity
{
    /**
     * @Id
     * @Column(type="integer")
     * @GeneratedValue
     */
    public $id;

    /**
     * @OneToMany(targetEntity="GH7642Entity", mappedBy="user")
     */
    public $stats;
}