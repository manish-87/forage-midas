package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class TransactionListener {
    private static final Logger logger = LoggerFactory.getLogger(TransactionListener.class);
    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;
    private final IncentiveRequester incentiveRequester;

    public TransactionListener(UserRepository userRepository, TransactionRepository transactionRepository,
            IncentiveRequester incentiveRequester) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
        this.incentiveRequester = incentiveRequester;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-group")
    @Transactional
    public void listen(Transaction transaction) {
        logger.info("Received transaction: {}", transaction);

        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        if (sender != null && recipient != null) {
            if (sender.getBalance() >= transaction.getAmount()) {
                Incentive incentive = incentiveRequester.getIncentive(transaction);
                float incentiveAmount = incentive.getAmount();

                sender.setBalance(sender.getBalance() - transaction.getAmount());
                recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);

                userRepository.save(sender);
                userRepository.save(recipient);

                TransactionRecord record = new TransactionRecord(sender, recipient, transaction.getAmount(),
                        incentiveAmount);
                transactionRepository.save(record);
                logger.info("Transaction processed successfully for senderId: {}, recipientId: {}", sender.getId(),
                        recipient.getId());
            } else {
                logger.info("Transaction rejected: Insufficient funds for senderId: {}", sender.getId());
            }
        } else {
            logger.info("Transaction rejected: Invalid senderId or recipientId");
        }
    }
}
